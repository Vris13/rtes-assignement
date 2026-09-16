#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <libwebsockets.h>
#include <cjson/cJSON.h>

#define MSG_MAX_LEN    8192

#define QUEUE_CAP      256
#define QUEUE_MASK     (QUEUE_CAP - 1)

#define BACKOFF_INIT   1000
#define BACKOFF_MAX    60000

#define WS_HOST   "jetstream1.us-east.bsky.network"
#define WS_PORT   443
#define WS_PATH   "/subscribe?wantedCollections=app.bsky.feed.post"
#define WS_PROTO  "lws-minimal"
#define LOG_FILE  "metrics_log.txt"

#define STALL_LIMIT  5

/* ------------------------------------------------------------------ */

typedef struct {
    char     slots[QUEUE_CAP][MSG_MAX_LEN];
    uint32_t head;
    uint32_t tail;
    uint32_t count;
} MsgQueue;

static MsgQueue g_queue;

static pthread_mutex_t g_qmtx  = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_qcond  = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_cmtx  = PTHREAD_MUTEX_INITIALIZER;

static volatile uint64_t g_commits   = 0;
static volatile uint64_t g_identities = 0;
static volatile uint64_t g_accounts  = 0;
static volatile uint64_t g_infos     = 0;

static volatile sig_atomic_t g_running    = 1;
static volatile int          g_connected  = 0;

/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t user, nice, system, idle, iowait, irq, softirq, steal;
} Jiffies;

static bool read_jiffies(Jiffies *j)
{
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return false;
    int n = fscanf(f,
        "cpu  %"SCNu64" %"SCNu64" %"SCNu64" %"SCNu64
             " %"SCNu64" %"SCNu64" %"SCNu64" %"SCNu64,
        &j->user, &j->nice, &j->system, &j->idle,
        &j->iowait, &j->irq, &j->softirq, &j->steal);
    fclose(f);
    return n == 8;
}

static double cpu_percent(const Jiffies *a, const Jiffies *b)
{
    uint64_t idle_a = a->idle + a->iowait;
    uint64_t idle_b = b->idle + b->iowait;
    uint64_t tot_a  = a->user + a->nice + a->system + a->idle
                    + a->iowait + a->irq + a->softirq + a->steal;
    uint64_t tot_b  = b->user + b->nice + b->system + b->idle
                    + b->iowait + b->irq + b->softirq + b->steal;

    uint64_t dtot  = tot_b - tot_a;
    uint64_t didle = idle_b - idle_a;

    if (dtot == 0) return 0.0;
    return (1.0 - (double)didle / (double)dtot) * 100.0;
}

/* ------------------------------------------------------------------ */
/* Producer - Thread 1                                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    char   buf[MSG_MAX_LEN];
    size_t len;
} WsConn;

static int ws_event(struct lws *wsi, enum lws_callback_reasons reason,
                    void *user, void *in, size_t len)
{
    WsConn *conn = (WsConn *)user;

    switch (reason) {

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        memset(conn->buf, 0, sizeof conn->buf);
        conn->len = 0;
        g_connected = 1;
        break;

    case LWS_CALLBACK_CLIENT_RECEIVE: {
        bool final = lws_is_final_fragment(wsi);
        size_t copy = len;

        if (conn->len + copy + 1 > MSG_MAX_LEN)
            copy = (conn->len < MSG_MAX_LEN - 1) ? MSG_MAX_LEN - 1 - conn->len : 0;

        if (copy > 0) {
            memcpy(conn->buf + conn->len, in, copy);
            conn->len += copy;
        }

        if (!final) break;

        conn->buf[conn->len] = '\0';

        pthread_mutex_lock(&g_qmtx);
        if (g_queue.count < QUEUE_CAP) {
            strncpy(g_queue.slots[g_queue.head], conn->buf, MSG_MAX_LEN - 1);
            g_queue.slots[g_queue.head][MSG_MAX_LEN - 1] = '\0';
            g_queue.head = (g_queue.head + 1) & QUEUE_MASK;
            g_queue.count++;
            pthread_cond_signal(&g_qcond);
        }
        pthread_mutex_unlock(&g_qmtx);

        conn->len = 0;
        break;
    }

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        g_connected = 0;
        return -1;

    case LWS_CALLBACK_CLIENT_CLOSED:
        g_connected = 0;
        return -1;
		
    default:
        break;
    }
    return 0;
}

static const struct lws_protocols ws_protocols[] = {
    { WS_PROTO, ws_event, sizeof(WsConn), MSG_MAX_LEN, 0, NULL, 0 },
    LWS_PROTOCOL_LIST_TERM
};

static void *producer(void *arg)
{
    (void)arg;
    lws_set_log_level(LLL_ERR | LLL_WARN, NULL);

    struct lws_context_creation_info ctx_info;
    struct lws_client_connect_info   conn_info;
    struct lws_context *ctx = NULL;
    long backoff = BACKOFF_INIT;

    memset(&ctx_info, 0, sizeof ctx_info);
    ctx_info.port        = CONTEXT_PORT_NO_LISTEN;
    ctx_info.protocols   = ws_protocols;
    ctx_info.options     = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    ctx_info.fd_limit_per_thread = 64;

    /* Create context ONCE outside the loop to prevent memory/DNS leaks on disconnects */
    ctx = lws_create_context(&ctx_info);
    if (!ctx) {
        fprintf(stderr, "Failed to create lws context\n");
        pthread_exit(NULL);
    }

    while (g_running) {
        memset(&conn_info, 0, sizeof conn_info);
        conn_info.context        = ctx;
        conn_info.address        = WS_HOST;
        conn_info.port           = WS_PORT;
        conn_info.path           = WS_PATH;
        conn_info.host           = WS_HOST;
        conn_info.origin         = WS_HOST;
        conn_info.protocol       = WS_PROTO;
        conn_info.ssl_connection = LCCSCF_USE_SSL;

        if (!lws_client_connect_via_info(&conn_info)) {
            goto retry;
        }

        int was_connected = 0;
        while (g_running) {
            lws_service(ctx, 100);

            if (g_connected) {
                was_connected = 1;
                backoff = BACKOFF_INIT; /* Reset backoff when healthy */
            } else if (was_connected) {
                break; /* We were connected but lost signal. Break out to retry. */
            }
        }

retry:
        g_connected = 0;
        struct timespec wait = {
            .tv_sec  = backoff / 1000,
            .tv_nsec = (backoff % 1000) * 1000000L
        };
        nanosleep(&wait, NULL);
        backoff = (backoff * 2 > BACKOFF_MAX) ? BACKOFF_MAX : backoff * 2;
    }

    if (ctx) lws_context_destroy(ctx);
    pthread_exit(NULL);
} 

/* ------------------------------------------------------------------ */
/* Consumer - Thread 2                        */
/* ------------------------------------------------------------------ */

static void *consumer(void *arg)
{
    (void)arg;
    char buf[MSG_MAX_LEN];

    while (g_running) {
        pthread_mutex_lock(&g_qmtx);
        while (g_queue.count == 0 && g_running)
            pthread_cond_wait(&g_qcond, &g_qmtx);

        if (!g_running && g_queue.count == 0) {
            pthread_mutex_unlock(&g_qmtx);
            break;
        }

        memcpy(buf, g_queue.slots[g_queue.tail], MSG_MAX_LEN);
        g_queue.tail = (g_queue.tail + 1) & QUEUE_MASK;
        g_queue.count--;
        pthread_mutex_unlock(&g_qmtx);

        cJSON *root = cJSON_ParseWithLength(buf, MSG_MAX_LEN);
        if (!root) continue;

        cJSON *kind = cJSON_GetObjectItemCaseSensitive(root, "kind");
        if (cJSON_IsString(kind) && kind->valuestring) {
            const char *k = kind->valuestring;
            pthread_mutex_lock(&g_cmtx);
            if      (strcmp(k, "commit")   == 0) g_commits++;
            else if (strcmp(k, "identity") == 0) g_identities++;
            else if (strcmp(k, "account")  == 0) g_accounts++;
            else if (strcmp(k, "info")     == 0) g_infos++;
            pthread_mutex_unlock(&g_cmtx);
        }

        cJSON_Delete(root);
    }

    pthread_exit(NULL);
}

/* ------------------------------------------------------------------ */
/* Monitor - Thread 3                                                   */
/* ------------------------------------------------------------------ */

static void *monitor(void *arg)
{
    (void)arg;

    FILE *fp = fopen(LOG_FILE, "a");
    if (!fp) { perror("fopen"); pthread_exit(NULL); }

    fseek(fp, 0, SEEK_END);
    if (ftell(fp) == 0) {
        fprintf(fp, "Seconds,Nanoseconds,Commit_Count,Identity_Count,"
                    "Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct,Status\n");
        fflush(fp);
    }

    Jiffies prev_j, curr_j;
    if (!read_jiffies(&prev_j))
        memset(&prev_j, 0, sizeof prev_j);


    struct timespec next;
    clock_gettime(CLOCK_MONOTONIC, &next);
    next.tv_sec += 1;

    int zero_streak = 0;

    while (g_running) {
        int rc = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);

        if (rc == EINTR) {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            if (now.tv_sec < next.tv_sec ||
                (now.tv_sec == next.tv_sec && now.tv_nsec < next.tv_nsec))
                continue;
        } else if (rc != 0) {
            clock_gettime(CLOCK_MONOTONIC, &next);
            next.tv_sec += 1;
            continue;
        }

        if (!g_running) break;

        next.tv_sec += 1;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        if (next.tv_sec < now.tv_sec ||
            (next.tv_sec == now.tv_sec && next.tv_nsec <= now.tv_nsec)) {
            next.tv_sec  = now.tv_sec + 1;
            next.tv_nsec = now.tv_nsec;
        }

        struct timespec wall;
        clock_gettime(CLOCK_REALTIME, &wall);

        pthread_mutex_lock(&g_cmtx);
        uint64_t c = g_commits;
        uint64_t i = g_identities;
        uint64_t a = g_accounts;
        uint64_t n = g_infos;
        g_commits    = 0;
        g_identities = 0;
        g_accounts   = 0;
        g_infos      = 0;
        pthread_mutex_unlock(&g_cmtx);

        pthread_mutex_lock(&g_qmtx);
        uint32_t qlen = g_queue.count;
        pthread_mutex_unlock(&g_qmtx);

        double occ = (double)qlen / (double)QUEUE_CAP * 100.0;

        double cpu = 0.0;
        if (read_jiffies(&curr_j)) {
            cpu = cpu_percent(&prev_j, &curr_j);
            prev_j = curr_j;
        }

        uint64_t total = c + i + a + n;
        const char *status;

        if (!g_connected) {
            zero_streak = 0;
            status = "LOST-SIGNAL";
        } else if (total == 0) {
            zero_streak++;
            status = (zero_streak >= STALL_LIMIT) ? "STALLED" : "OK";
        } else {
            zero_streak = 0;
            status = "OK";
        }

        fprintf(fp, "%ld,%ld,%"PRIu64",%"PRIu64",%"PRIu64",%"PRIu64",%.2f,%.2f,%s\n",
                (long)wall.tv_sec, (long)wall.tv_nsec,
                c, i, a, n, occ, cpu, status);
        fflush(fp);
    }

    fclose(fp);
    pthread_exit(NULL);
}

/* ------------------------------------------------------------------ */

static void handle_signal(int sig)
{
    (void)sig;
    g_running = 0;
    pthread_cond_broadcast(&g_qcond);
}

int main(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    memset(&g_queue, 0, sizeof g_queue);

    pthread_t t_prod, t_cons, t_mon;

#define TRY_CREATE(t, fn, name) do { \
    int r = pthread_create(&(t), NULL, fn, NULL); \
    if (r) { fprintf(stderr, "pthread_create %s: %s\n", name, strerror(r)); return 1; } \
} while (0)

    TRY_CREATE(t_mon,  monitor,  "monitor");
    TRY_CREATE(t_cons, consumer, "consumer");
    TRY_CREATE(t_prod, producer, "producer");

    printf("Ξεκίνησε. Log: %s — Ctrl-C για τερματισμό.\n", LOG_FILE);

    pthread_join(t_prod, NULL);
    pthread_join(t_cons, NULL);
    pthread_join(t_mon,  NULL);

    pthread_mutex_destroy(&g_qmtx);
    pthread_mutex_destroy(&g_cmtx);
    pthread_cond_destroy(&g_qcond);

    printf("Τερματισμός.\n");
    return 0;
}
