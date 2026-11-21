#ifndef SHOPPING_H
#define SHOPPING_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <pthread.h>

#define DAY_MS INT64_C(86400000)
#define WEEK_MS (7 * DAY_MS)
#define GAP_MS INT64_C(1800000)
#define MAX_WORKERS 8
#define EVENT_SHARDS 256
#define PERSON_SHARDS 128
#define NO_CATEGORY UINT32_MAX

typedef struct { const unsigned char *p; size_t n; } Slice;
typedef uint64_t Key;
typedef struct {
    Key *keys;
    uint64_t *values, *presence;
    unsigned char *control;
    size_t capacity, used, presence_mask;
} Map;
typedef struct { int64_t start, end; int first_year, years; int32_t *dates; } Window;

enum ScanKind { SCAN_SCALAR, SCAN_AVX2, SCAN_AVX512, SCAN_ASM };
enum ReadKind { READ_MAP, READ_PREAD, READ_URING, READ_DIRECT };
enum DedupKind { DEDUP_FILTER, DEDUP_OWNER, DEDUP_TWO_SCAN, DEDUP_EAGER };
enum CodecKind { CODEC_SNAPPY, CODEC_NONE, CODEC_LZ4 };
enum ClickKind { CLICK_BOUNDS, CLICK_ENVELOPE, CLICK_SWEEP };
enum ReadHint { HINT_NONE, HINT_SEQUENTIAL, HINT_WILLNEED };

typedef struct {
    enum ScanKind scan;
    enum ReadKind read;
    enum DedupKind dedup;
    enum CodecKind codec;
    enum ClickKind click;
    enum ReadHint read_hint;
    size_t chunk, click_batch, row_group;
    unsigned tiny, bucket_min, scan_unroll;
    bool calendar, digits_simd, compact_ids, cache_fields, packed_events, bucket_bits, mask_days, overlap, dictionary;
    bool grouped_map, alias_filter, alias_route, tiny_simd, static_jobs, plain_writer, profile;
} Options;

extern Options options;

_Noreturn void fail(const char *format, ...);
void *allocate(size_t n, size_t size);
void *zero_allocate(size_t n, size_t size);
void *resize_array(void *ptr, size_t n, size_t size);
void *grow(void *ptr, size_t *capacity, size_t size);
char *path_join(const char *dir, const char *name);

#define VECTOR(type, name) typedef struct { type *v; size_t n, cap; } name
#define PUSH(a, value) do { \
    if ((a)->n == (a)->cap) (a)->v = grow((a)->v, &(a)->cap, sizeof *(a)->v); \
    (a)->v[(a)->n++] = (value); \
} while (0)

static inline uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    return x ^ (x >> 31);
}

uint64_t lookup_grouped(const Map *m, Key key, uint64_t hash, uint64_t missing);

static inline uint64_t lookup(const Map *m, Key key, uint64_t missing) {
    if (!m->capacity || !key) return missing;
    uint64_t hash = mix(key);
    if (m->presence) {
        size_t bit = hash & m->presence_mask;
        if (!(m->presence[bit / 64] & (UINT64_C(1) << (bit % 64)))) return missing;
    }
    if (m->control) return lookup_grouped(m, key, hash, missing);
    size_t i = hash & (m->capacity - 1);
    while (m->keys[i]) {
        if (m->keys[i] == key) return m->values[i];
        i = (i + 1) & (m->capacity - 1);
    }
    return missing;
}

uint64_t *entry(Map *m, Key key, uint64_t initial);
void destroy_map(Map *m);
void freeze_presence(Map *m);
void init_keys(void);
Key intern(Slice text);
Slice key_bytes(Key key, unsigned char bytes[10]);
int compare_key(Key a, Key b);
bool text_is(Slice text, const char *literal, size_t n);

int32_t utc_day(int64_t ms);
bool number(Slice text, int64_t limit, int64_t *result);
bool timestamp(Slice text, const Window *window, bool ingest, int64_t *result);
void init_calendar(Window *window, int start_year, int end_year);

typedef void (*Task)(void *, size_t, unsigned);
typedef struct {
    pthread_barrier_t begin, end;
    pthread_t threads[MAX_WORKERS - 1];
    atomic_size_t next;
    size_t work;
    unsigned workers;
    Task task;
    void *data;
} Pool;

void init_pool(Pool *pool, unsigned workers);
void parallel(Pool *pool, size_t jobs, Task task, void *data);
void destroy_pool(Pool *pool);
void phase(const char *label);

typedef struct {
    char *path;
    int fd, direct_fd;
    unsigned char *data;
    size_t size;
    bool mapped;
} InputFile;

typedef struct { InputFile *file; size_t start, end; } Range;
typedef struct { InputFile *files; size_t count; Range *ranges; size_t nranges, bytes; } Input;
typedef void (*RowVisitor)(void *, unsigned, Slice, const Slice *);

Input open_input(const char *directory, const char *file);
void load_input(Input *input, Pool *pool);
void close_input(Input *input);
bool parse_fields(Slice row, Slice *fields, unsigned columns);
void scan_range(const Range *range, unsigned columns, RowVisitor visit, void *ctx, unsigned worker);

typedef struct { Key id, channel; } Campaign;
typedef struct {
    Map products, campaigns;
    Key *categories;
    size_t category_count;
    uint32_t unknown;
    Campaign *names;
    size_t campaign_count;
} Catalog;

typedef struct { int64_t start, revenue; uint32_t events; } Session;
typedef struct { size_t begin, end; } Person;
typedef struct { Key person; int64_t revenue, events, sessions; int32_t day; uint32_t category; } UserDay;
typedef struct { int64_t sessions, purchasing, revenue, assists; } Metric;
VECTOR(UserDay, UserDays);
VECTOR(Session, Sessions);

typedef struct { Map index; Person *people; size_t count; Sessions sessions; UserDays days; } Timeline;
typedef struct { uint32_t campaign; int32_t day; Metric values; } CampaignDay;
VECTOR(CampaignDay, CampaignDays);

void build_sessions(const char *data, Pool *pool, const Window *window, Catalog *catalog, Map *aliases, Timeline *people);
CampaignDays attribute(const char *data, const char *output, Pool *pool, const Window *window, const Catalog *catalog, const Map *aliases, const Timeline *people);
void prepare_output(const char *output);
void write_users(const char *output, const Catalog *catalog, const Timeline *people, size_t first, size_t end, unsigned shard);
void write_campaigns(const char *output, const Catalog *catalog, const CampaignDays *days);

#endif
