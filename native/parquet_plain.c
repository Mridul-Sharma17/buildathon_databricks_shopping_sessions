#include "shopping.h"
#include "parquet_plain.h"

#include <lz4.h>
#include <stdio.h>
#include <string.h>

/* Parquet uses Thrift Compact Protocol headers and little-endian PLAIN values. */
enum { CT_STOP, CT_I32 = 5, CT_I64 = 6, CT_BINARY = 8, CT_LIST = 9, CT_STRUCT = 12 };
enum { PLAIN_ENCODING = 0, RLE_ENCODING = 3, PAGE_BYTES = 1024 * 1024 };
VECTOR(unsigned char, Bytes);
typedef struct { uint64_t offset, stored, raw; } Chunk;
typedef struct { Chunk chunks[7]; size_t rows; } Group;
VECTOR(Group, Groups);

struct PlainWriter {
    FILE *file;
    const char *path;
    const Column *columns;
    size_t count, next;
    bool lz4;
    uint64_t offset, rows;
    Group current;
    Groups groups;
    Bytes data, compressed, meta;
};

static void append(Bytes *b, const void *values, size_t n) {
    if (!n) return;
    if (n > SIZE_MAX - b->n) fail("Parquet buffer size overflow");
    while (b->cap < b->n + n) b->v = grow(b->v, &b->cap, 1);
    memcpy(b->v + b->n, values, n);
    b->n += n;
}

static void byte(Bytes *b, unsigned value) { PUSH(b, (unsigned char)value); }

static void varint(Bytes *b, uint64_t value) {
    while (value >= 128) { byte(b, (unsigned)(value | 128)); value >>= 7; }
    byte(b, (unsigned)value);
}

static void unsigned_le32(Bytes *b, uint32_t value) {
    unsigned char bytes[] = {(unsigned char)value, (unsigned char)(value >> 8),
        (unsigned char)(value >> 16), (unsigned char)(value >> 24)};
    append(b, bytes, sizeof bytes);
}

static void field(Bytes *b, unsigned delta, unsigned kind) { byte(b, (delta << 4) | kind); }

static void unsigned_field(Bytes *b, unsigned delta, unsigned kind, uint64_t value) {
    if (value > INT64_MAX) fail("Parquet metadata integer overflow");
    field(b, delta, kind);
    varint(b, value << 1);
}

static void text(Bytes *b, const char *value) {
    size_t n = strlen(value);
    varint(b, n);
    append(b, value, n);
}

static void list(Bytes *b, size_t count, unsigned kind) {
    if (count < 15) byte(b, (unsigned)(count << 4) | kind);
    else { byte(b, 0xf0 | kind); varint(b, count); }
}

static void write_bytes(PlainWriter *w, const void *values, size_t n) {
    if (n && fwrite(values, 1, n, w->file) != n) fail("write failed: %s", w->path);
    w->offset += n;
}

PlainWriter *plain_open(const char *path, const Column *columns, size_t count, bool lz4) {
    if (count > 7) fail("too many plain Parquet columns");
    PlainWriter *w = zero_allocate(1, sizeof *w);
    w->file = fopen(path, "wb");
    if (!w->file) fail("cannot open %s", path);
    w->path = path;
    w->columns = columns;
    w->count = count;
    w->lz4 = lz4;
    write_bytes(w, "PAR1", 4);
    return w;
}

static void levels(Bytes *b, const int16_t *defined, size_t rows) {
    size_t groups = (rows + 7) / 8;
    unsigned char prefix[10];
    Bytes header = { .v = prefix, .cap = sizeof prefix };
    varint(&header, ((uint64_t)groups << 1) | 1);
    unsigned_le32(b, (uint32_t)(groups + header.n));
    append(b, prefix, header.n);
    for (size_t i = 0; i < rows; i += 8) {
        unsigned bits = 0;
        for (size_t j = 0; j < 8 && i + j < rows; ++j) bits |= (unsigned)(defined[i + j] != 0) << j;
        byte(b, bits);
    }
}

static void page(PlainWriter *w, size_t rows, Chunk *meta) {
    size_t raw = w->data.n, stored = raw;
    const unsigned char *payload = w->data.v;
    if (raw > INT32_MAX) fail("Parquet page size overflow: %s", w->path);
    if (w->lz4) {
        int cap = LZ4_compressBound((int)raw);
        if (cap <= 0) fail("LZ4 page size out of range: %s", w->path);
        while (w->compressed.cap < (size_t)cap) w->compressed.v = grow(w->compressed.v, &w->compressed.cap, 1);
        int n = LZ4_compress_default((const char *)w->data.v, (char *)w->compressed.v, (int)raw, cap);
        if (n <= 0) fail("LZ4 compression failed: %s", w->path);
        payload = w->compressed.v;
        stored = (size_t)n;
    }
    Bytes *header = &w->meta;
    header->n = 0;
    unsigned_field(header, 1, CT_I32, 0); /* DATA_PAGE */
    unsigned_field(header, 1, CT_I32, raw);
    unsigned_field(header, 1, CT_I32, stored);
    field(header, 2, CT_STRUCT); /* data_page_header */
    unsigned_field(header, 1, CT_I32, rows);
    unsigned_field(header, 1, CT_I32, PLAIN_ENCODING);
    unsigned_field(header, 1, CT_I32, RLE_ENCODING);
    unsigned_field(header, 1, CT_I32, RLE_ENCODING);
    byte(header, CT_STOP);
    byte(header, CT_STOP);
    write_bytes(w, header->v, header->n);
    write_bytes(w, payload, stored);
    meta->raw += header->n + raw;
    meta->stored += header->n + stored;
}

void plain_column(PlainWriter *w, unsigned column, const void *values, size_t rows, const int16_t *defined) {
    if (column != w->next || column >= w->count || (column && w->current.rows != rows))
        fail("inconsistent Parquet column batch: %s", w->path);
    const Column *c = &w->columns[column];
    Chunk *meta = &w->current.chunks[column];
    meta->offset = w->offset;
    w->current.rows = rows;
    ++w->next;
    for (size_t offset = 0, physical = 0; offset < rows;) {
        size_t n = rows - offset, present = n;
        w->data.n = 0;
        if (c->physical == CARQUET_PHYSICAL_BYTE_ARRAY) {
            const carquet_byte_array_t *strings = values;
            size_t bytes = 0;
            n = present = 0;
            while (offset + n < rows) {
                size_t next = 0;
                if (!defined || defined[offset + n]) {
                    int32_t length = strings[physical + present].length;
                    if (length < 0) fail("invalid Parquet byte array");
                    next = (size_t)length + 4;
                }
                if (n && bytes + next > PAGE_BYTES) break;
                present += next != 0;
                bytes += next;
                ++n;
            }
            if (defined) levels(&w->data, defined + offset, n);
            for (size_t i = 0; i < present; ++i) {
                carquet_byte_array_t s = strings[physical + i];
                unsigned_le32(&w->data, (uint32_t)s.length);
                append(&w->data, s.data, (size_t)s.length);
            }
        } else {
            size_t width = c->physical == CARQUET_PHYSICAL_INT32 ? 4 : 8;
            if (n > PAGE_BYTES / width) n = PAGE_BYTES / width;
            present = n;
            append(&w->data, (const unsigned char *)values + physical * width, n * width);
        }
        page(w, n, meta);
        offset += n;
        physical += present;
    }
}

void plain_flush(PlainWriter *w) {
    if (w->next != w->count) fail("incomplete Parquet row group: %s", w->path);
    PUSH(&w->groups, w->current);
    w->rows += w->current.rows;
    w->current = (Group){0};
    w->next = 0;
}

static void schema(Bytes *s, const Column *c) {
    unsigned_field(s, 1, CT_I32, c->physical);
    unsigned_field(s, 2, CT_I32, c->optional);
    field(s, 1, CT_BINARY);
    text(s, c->name);
    if (c->logical) {
        unsigned_field(s, 2, CT_I32, c->logical == CARQUET_LOGICAL_DATE ? 6 : 0);
        field(s, 4, CT_STRUCT); /* LogicalType union */
        field(s, c->logical == CARQUET_LOGICAL_DATE ? 6 : 1, CT_STRUCT);
        byte(s, CT_STOP);
        byte(s, CT_STOP);
    }
    byte(s, CT_STOP);
}

static void chunk(Bytes *s, const Column *column, Chunk c, size_t rows, bool lz4) {
    unsigned_field(s, 2, CT_I64, c.offset);
    field(s, 1, CT_STRUCT); /* ColumnMetaData */
    unsigned_field(s, 1, CT_I32, column->physical);
    field(s, 1, CT_LIST);
    list(s, 2, CT_I32);
    varint(s, PLAIN_ENCODING << 1);
    varint(s, RLE_ENCODING << 1);
    field(s, 1, CT_LIST);
    list(s, 1, CT_BINARY);
    text(s, column->name);
    unsigned_field(s, 1, CT_I32, lz4 ? 7 : 0);
    unsigned_field(s, 1, CT_I64, rows);
    unsigned_field(s, 1, CT_I64, c.raw);
    unsigned_field(s, 1, CT_I64, c.stored);
    unsigned_field(s, 2, CT_I64, c.offset);
    byte(s, CT_STOP);
    byte(s, CT_STOP);
}

void plain_close(PlainWriter *w) {
    if (w->next) plain_flush(w);
    Bytes *s = &w->meta;
    s->n = 0;
    unsigned_field(s, 1, CT_I32, 1);
    field(s, 1, CT_LIST);
    list(s, w->count + 1, CT_STRUCT);
    field(s, 4, CT_BINARY);
    text(s, "schema");
    unsigned_field(s, 1, CT_I32, w->count);
    byte(s, CT_STOP);
    for (size_t i = 0; i < w->count; ++i) schema(s, &w->columns[i]);
    unsigned_field(s, 1, CT_I64, w->rows);
    field(s, 1, CT_LIST);
    list(s, w->groups.n, CT_STRUCT);
    for (size_t i = 0; i < w->groups.n; ++i) {
        const Group *g = &w->groups.v[i];
        uint64_t raw = 0, stored = 0;
        field(s, 1, CT_LIST);
        list(s, w->count, CT_STRUCT);
        for (size_t j = 0; j < w->count; ++j) {
            Chunk c = g->chunks[j];
            chunk(s, &w->columns[j], c, g->rows, w->lz4);
            raw += c.raw;
            stored += c.stored;
        }
        unsigned_field(s, 1, CT_I64, raw);
        unsigned_field(s, 1, CT_I64, g->rows);
        unsigned_field(s, 3, CT_I64, stored);
        byte(s, CT_STOP);
    }
    field(s, 2, CT_BINARY);
    text(s, "shopping-sessions-c");
    byte(s, CT_STOP);
    if (s->n > UINT32_MAX) fail("Parquet footer exceeds uint32");
    uint32_t footer = (uint32_t)s->n;
    unsigned_le32(s, footer);
    append(s, "PAR1", 4);
    write_bytes(w, s->v, s->n);
    if (fclose(w->file)) fail("cannot close %s", w->path);
    free(w->groups.v);
    free(w->data.v);
    free(w->compressed.v);
    free(w->meta.v);
    free(w);
}
