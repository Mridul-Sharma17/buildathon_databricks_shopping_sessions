#include "shopping.h"
#include "parquet_plain.h"

#include <carquet/carquet.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static void mkdir_output(const char *path) {
    if (mkdir(path, 0777) == 0) return;
    if (errno != EEXIST) fail("cannot create %s: %s", path, strerror(errno));
    struct stat st;
    if (stat(path, &st) || !S_ISDIR(st.st_mode)) fail("output is not a directory: %s", path);
}

void prepare_output(const char *output) {
    mkdir_output(output);
    char *user = path_join(output, "user_daily"), *campaign = path_join(output, "campaign_daily");
    mkdir_output(user);
    mkdir_output(campaign);
    free(user);
    free(campaign);
}

typedef struct { carquet_writer_t *writer; carquet_schema_t *schema; PlainWriter *plain; char *path, *temp; } Writer;

static void checked(carquet_status_t code, const char *operation, const char *path) {
    if (code != CARQUET_OK) fail("Parquet %s failed (%d): %s", operation, (int)code, path);
}

static Writer writer_open(const char *out, const char *table, unsigned shard, const Column *columns, size_t count) {
    char part[48], temp[48];
    snprintf(part, sizeof part, "part-%u.parquet", shard);
    snprintf(temp, sizeof temp, ".part-%u.XXXXXX", shard);
    char *directory = path_join(out, table);
    Writer w = {.path = path_join(directory, part), .temp = path_join(directory, temp)};
    free(directory);
    int fd = mkstemp(w.temp);
    if (fd < 0 || close(fd)) fail("cannot create %s: %s", w.temp, strerror(errno));
    if (options.plain_writer) {
        w.plain = plain_open(w.temp, columns, count, options.codec == CODEC_LZ4);
        return w;
    }
    carquet_error_t error = CARQUET_ERROR_INIT;
    w.schema = carquet_schema_create(&error);
    if (!w.schema) fail("cannot build Parquet schema: %s", error.message);
    for (size_t i = 0; i < count; ++i) {
        const Column *c = &columns[i];
        carquet_logical_type_t logical = {.id = c->logical};
        checked(carquet_schema_add_column(w.schema, c->name, c->physical, c->logical ? &logical : NULL,
            c->optional ? CARQUET_REPETITION_OPTIONAL : CARQUET_REPETITION_REQUIRED, 0, 0), "schema", w.path);
    }
    carquet_writer_options_t opts;
    carquet_writer_options_init(&opts);
    opts.compression = options.codec == CODEC_NONE ? CARQUET_COMPRESSION_UNCOMPRESSED :
        options.codec == CODEC_LZ4 ? CARQUET_COMPRESSION_LZ4_RAW : CARQUET_COMPRESSION_SNAPPY;
    opts.row_group_size = INT64_MAX;
    opts.write_statistics = false;
    opts.write_crc = false;
    w.writer = carquet_writer_create(w.temp, w.schema, &opts, &error);
    if (!w.writer) fail("cannot create Parquet %s: %s", w.path, error.message);
    if (options.dictionary) {
        for (size_t i = 0; i < count; ++i) {
            const Column *c = &columns[i];
            if (c->logical == CARQUET_LOGICAL_DATE || (c->logical == CARQUET_LOGICAL_STRING && strcmp(c->name, "user_id")))
                checked(carquet_writer_set_column_encoding(w.writer, (int32_t)i, CARQUET_ENCODING_RLE_DICTIONARY), "encoding", w.path);
        }
    }
    return w;
}

static void writer_close(Writer *w) {
    if (w->plain) plain_close(w->plain);
    else {
        checked(carquet_writer_close(w->writer), "close", w->path);
        carquet_schema_free(w->schema);
    }
    if (rename(w->temp, w->path)) fail("cannot publish %s: %s", w->path, strerror(errno));
    free(w->temp);
    free(w->path);
}

static carquet_byte_array_t string(Key key, unsigned char *storage) {
    Slice s = key_bytes(key, storage);
    if (s.n > INT32_MAX) fail("Parquet string length exceeds int32");
    return (carquet_byte_array_t){.data = (unsigned char *)s.p, .length = (int32_t)s.n};
}

static void column(Writer *w, int id, const void *values, size_t n, const int16_t *definitions) {
    if (w->plain) plain_column(w->plain, (unsigned)id, values, n, definitions);
    else checked(carquet_writer_write_batch(w->writer, id, values, (int64_t)n, definitions, NULL), "write", w->path);
}

static void flush_group(Writer *w) {
    if (w->plain) plain_flush(w->plain);
    else checked(carquet_writer_new_row_group(w->writer), "row-group flush", w->path);
}

typedef struct {
    carquet_byte_array_t *first, *second;
    unsigned char *storage1, *storage2;
    int32_t *days;
    int64_t *numeric;
    int16_t *definitions;
    size_t capacity, n, strings;
} Columns;

static Columns batch(size_t count, bool optional) {
    return (Columns){
        .capacity = count,
        .first = allocate(count, sizeof(carquet_byte_array_t)), .second = allocate(count, sizeof(carquet_byte_array_t)),
        .storage1 = allocate(count, 10), .storage2 = allocate(count, 10),
        .days = allocate(count, sizeof(int32_t)), .numeric = allocate(count, 4 * sizeof(int64_t)),
        .definitions = optional ? allocate(count, sizeof(int16_t)) : NULL,
    };
}

static void release(Columns *c) {
    free(c->first);
    free(c->second);
    free(c->storage1);
    free(c->storage2);
    free(c->days);
    free(c->numeric);
    free(c->definitions);
}

static void user_batch(Writer *w, Columns *c) {
    if (!c->n) return;
    column(w, 0, c->first, c->n, NULL);
    column(w, 1, c->days, c->n, NULL);
    for (unsigned i = 0; i < 3; ++i) column(w, (int)i + 2, c->numeric + i * c->capacity, c->n, NULL);
    column(w, 5, c->second, c->n, c->definitions);
    flush_group(w);
    c->n = c->strings = 0;
}

void write_users(const char *out, const Catalog *catalog, const Timeline *people, size_t first, size_t end, unsigned shard) {
    static const Column schema[] = {
        {"user_id", CARQUET_PHYSICAL_BYTE_ARRAY, CARQUET_LOGICAL_STRING, false},
        {"day", CARQUET_PHYSICAL_INT32, CARQUET_LOGICAL_DATE, false},
        {"sessions", CARQUET_PHYSICAL_INT64, 0, false},
        {"events", CARQUET_PHYSICAL_INT64, 0, false},
        {"revenue_cents", CARQUET_PHYSICAL_INT64, 0, false},
        {"top_category", CARQUET_PHYSICAL_BYTE_ARRAY, CARQUET_LOGICAL_STRING, true},
    };
    Writer w = writer_open(out, "user_daily", shard, schema, sizeof schema / sizeof *schema);
    Columns c = batch(options.row_group, true);
    for (size_t i = first; i < end; ++i) {
        for (size_t j = 0; j < people[i].days.n; ++j) {
            const UserDay *row = &people[i].days.v[j];
            size_t pos = c.n++;
            c.first[pos] = string(row->person, c.storage1 + 10 * pos);
            c.days[pos] = row->day;
            c.numeric[pos] = row->sessions;
            c.numeric[c.capacity + pos] = row->events;
            c.numeric[2 * c.capacity + pos] = row->revenue;
            c.definitions[pos] = row->category != NO_CATEGORY;
            if (c.definitions[pos]) {
                size_t item = c.strings++;
                c.second[item] = string(catalog->categories[row->category], c.storage2 + 10 * item);
            }
            if (c.n == c.capacity) user_batch(&w, &c);
        }
    }
    user_batch(&w, &c);
    writer_close(&w);
    release(&c);
}

void write_campaigns(const char *out, const Catalog *catalog, const CampaignDays *days) {
    static const Column schema[] = {
        {"campaign_id", CARQUET_PHYSICAL_BYTE_ARRAY, CARQUET_LOGICAL_STRING, false},
        {"channel", CARQUET_PHYSICAL_BYTE_ARRAY, CARQUET_LOGICAL_STRING, false},
        {"day", CARQUET_PHYSICAL_INT32, CARQUET_LOGICAL_DATE, false},
        {"sessions", CARQUET_PHYSICAL_INT64, 0, false},
        {"purchasing_sessions", CARQUET_PHYSICAL_INT64, 0, false},
        {"revenue_cents", CARQUET_PHYSICAL_INT64, 0, false},
        {"assisted_sessions", CARQUET_PHYSICAL_INT64, 0, false},
    };
    Writer w = writer_open(out, "campaign_daily", 0, schema, sizeof schema / sizeof *schema);
    Columns c = batch(options.row_group, false);
    for (size_t start = 0; start < days->n;) {
        size_t n = days->n - start;
        if (n > c.capacity) n = c.capacity;
        for (size_t i = 0; i < n; ++i) {
            const CampaignDay *row = &days->v[start + i];
            Campaign details = catalog->names[row->campaign];
            c.first[i] = string(details.id, c.storage1 + 10 * i);
            c.second[i] = string(details.channel, c.storage2 + 10 * i);
            c.days[i] = row->day;
            c.numeric[i] = row->values.sessions;
            c.numeric[c.capacity + i] = row->values.purchasing;
            c.numeric[2 * c.capacity + i] = row->values.revenue;
            c.numeric[3 * c.capacity + i] = row->values.assists;
        }
        column(&w, 0, c.first, n, NULL);
        column(&w, 1, c.second, n, NULL);
        column(&w, 2, c.days, n, NULL);
        for (unsigned i = 0; i < 4; ++i) column(&w, (int)i + 3, c.numeric + i * c.capacity, n, NULL);
        flush_group(&w);
        start += n;
    }
    writer_close(&w);
    release(&c);
}
