#ifndef PARQUET_PLAIN_H
#define PARQUET_PLAIN_H

#include <carquet/carquet.h>

typedef struct { const char *name; carquet_physical_type_t physical; carquet_logical_type_id_t logical; bool optional; } Column;
typedef struct PlainWriter PlainWriter;

PlainWriter *plain_open(const char *path, const Column *columns, size_t count, bool lz4);
void plain_column(PlainWriter *writer, unsigned column, const void *values, size_t rows, const int16_t *definitions);
void plain_flush(PlainWriter *writer);
void plain_close(PlainWriter *writer);

#endif
