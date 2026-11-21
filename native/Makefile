CC ?= cc
CARQUET_SRC ?= /opt/carquet
CARQUET_LIB ?= /opt/carquet-build/libcarquet.a
TARGET ?= shopping-sessions
CFLAGS ?= -O3 -march=x86-64-v3 -flto
LIBS ?= -lz -lzstd -llz4 -lm -ljson-c -luring

SOURCES := main.c base.c input.c events.c attribution.c output.c parquet_plain.c

$(TARGET): $(SOURCES) shopping.h parquet_plain.h Makefile $(CARQUET_LIB)
	$(CC) -std=gnu11 -D_GNU_SOURCE -pthread -Wall -Wextra -Werror $(CFLAGS) $(CPPFLAGS) \
	  -I$(CARQUET_SRC)/include $(SOURCES) $(CARQUET_LIB) $(LDFLAGS) $(LIBS) -o $@
