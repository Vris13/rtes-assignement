CC ?= gcc
CFLAGS ?= -O2 -Wall -Wextra -std=c11
CPPFLAGS += $(shell pkg-config --cflags libwebsockets libcjson 2>/dev/null)
LDFLAGS +=
LDLIBS += $(shell pkg-config --libs libwebsockets libcjson 2>/dev/null)
LDLIBS += -lwebsockets -lcjson -lpthread -lm

TARGET := jetstream

all: $(TARGET)

$(TARGET): jetstream.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
