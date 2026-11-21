#include "shopping.h"

#include <errno.h>
#include <json-c/json.h>
#include <sched.h>
#include <string.h>

static int selection(const char *name, const char *const *values, size_t count, int fallback) {
    const char *text = getenv(name);
    if (!text || !*text) return fallback;
    for (size_t i = 0; i < count; ++i) if (!strcmp(text, values[i])) return (int)i;
    fail("invalid %s: %s", name, text);
}

static size_t setting(const char *name, size_t fallback, size_t min, size_t max) {
    const char *text = getenv(name);
    if (!text || !*text) return fallback;
    char *end;
    errno = 0;
    unsigned long long value = strtoull(text, &end, 10);
    if (errno || end == text || *end || value < min || value > max) fail("invalid %s: %s", name, text);
    return (size_t)value;
}

static void configure(void) {
    enum CodecKind codec = selection("SHOP_CODEC", (const char *const[]){"snappy", "none", "lz4"}, 3, CODEC_NONE);
    options = (Options){
        .scan = selection("SHOP_SCAN", (const char *const[]){"scalar", "avx2", "avx512", "asm"}, 4, SCAN_ASM),
        .read = selection("SHOP_IO", (const char *const[]){"mmap", "pread", "uring", "direct"}, 4, READ_MAP),
        .dedup = selection("SHOP_DEDUP", (const char *const[]){"filter", "owner", "two-scan", "eager"}, 4, DEDUP_EAGER),
        .codec = codec,
        .click = selection("SHOP_CLICK_MODE", (const char *const[]){"bounds", "envelope", "sweep"}, 3, CLICK_BOUNDS),
        .read_hint = selection("SHOP_READ_HINT", (const char *const[]){"none", "sequential", "willneed"}, 3, HINT_SEQUENTIAL),
        .chunk = setting("SHOP_CHUNK_MIB", 16, 1, 32) * 1024 * 1024,
        .click_batch = setting("SHOP_CLICK_BATCH", 32, 0, SIZE_MAX),
        .row_group = setting("SHOP_ROW_GROUP", 65536, 16, 1048576),
        .tiny = (unsigned)setting("SHOP_TINY_SESSIONS", 8, 0, 64),
        .bucket_min = (unsigned)setting("SHOP_BUCKET_MIN", 64, 1, UINT32_MAX),
        .scan_unroll = (unsigned)setting("SHOP_SCAN_UNROLL", 1, 1, 4),
        .calendar = setting("SHOP_CALENDAR", 1, 0, 1),
        .digits_simd = setting("SHOP_SIMD_DIGITS", 1, 0, 1),
        .compact_ids = setting("SHOP_COMPACT_IDS", 1, 0, 1),
        .cache_fields = setting("SHOP_CACHE_FIELDS", 1, 0, 1),
        .packed_events = setting("SHOP_PACKED_EVENTS", 1, 0, 1),
        .bucket_bits = setting("SHOP_BUCKET_BITS", 1, 0, 1),
        .mask_days = setting("SHOP_DAY_MASKS", 1, 0, 1),
        .overlap = setting("SHOP_WRITE_OVERLAP", 1, 0, 1),
        .dictionary = setting("SHOP_DICTIONARY", 1, 0, 1),
        .grouped_map = setting("SHOP_GROUPED_MAP", 1, 0, 1),
        .alias_filter = setting("SHOP_ALIAS_FILTER", 1, 0, 1),
        .alias_route = setting("SHOP_ALIAS_ROUTE", 0, 0, 1),
        .tiny_simd = setting("SHOP_TINY_SIMD", 0, 0, 1),
        .static_jobs = setting("SHOP_STATIC_JOBS", 0, 0, 1),
        .plain_writer = selection("SHOP_WRITER", (const char *const[]){"carquet", "plain"}, 2, codec != CODEC_SNAPPY) != 0,
        .profile = setting("SHOP_PROFILE", 0, 0, 1),
    };
    if (options.scan == SCAN_AVX512 && !__builtin_cpu_supports("avx512bw"))
        options.scan = __builtin_cpu_supports("avx2") ? SCAN_AVX2 : SCAN_SCALAR;
    if ((options.scan == SCAN_AVX2 || options.scan == SCAN_ASM) && !__builtin_cpu_supports("avx2"))
        options.scan = SCAN_SCALAR;
    if (options.plain_writer && options.codec == CODEC_SNAPPY)
        fail("the PLAIN writer supports SHOP_CODEC=none or SHOP_CODEC=lz4");
}

static int64_t manifest_bound(json_object *json, const char *name, int *year) {
    json_object *field;
    if (!json_object_object_get_ex(json, name, &field) || !json_object_is_type(field, json_type_string))
        fail("invalid manifest bound %s", name);
    Slice value = {(const unsigned char *)json_object_get_string(field), (size_t)json_object_get_string_len(field)};
    int64_t result;
    if (value.n != 20 || value.p[19] != 'Z' || !timestamp(value, NULL, false, &result))
        fail("invalid manifest bound %s", name);
    *year = (value.p[0] - '0') * 1000 + (value.p[1] - '0') * 100 + (value.p[2] - '0') * 10 + value.p[3] - '0';
    return result;
}

static Window manifest(const char *data) {
    char *path = path_join(data, "manifest.json");
    json_object *json = json_object_from_file(path);
    if (!json) fail("cannot read manifest %s", path);
    free(path);
    int first, last;
    Window w = {.start = manifest_bound(json, "window_start", &first), .end = manifest_bound(json, "window_end", &last)};
    json_object_put(json);
    if (w.start >= w.end) fail("manifest window is empty or reversed");
    init_calendar(&w, first > 0 ? first - 1 : first, last < 9999 ? last + 1 : last);
    return w;
}

typedef struct { const char *output; const Catalog *catalog; const Timeline *people; } WriteJob;

static void write_shard(void *data, size_t part, unsigned worker) {
    (void)worker;
    const WriteJob *job = data;
    size_t width = PERSON_SHARDS / MAX_WORKERS;
    write_users(job->output, job->catalog, job->people, part * width, (part + 1) * width, (unsigned)part);
}

int main(void) {
    configure();
    phase(NULL);
    const char *data = getenv("DATA_DIR"), *output = getenv("OUTPUT_DIR");
    if (!data) data = "/data";
    if (!output) output = "/output";
    unsigned workers = (unsigned)setting("NCPU", MAX_WORKERS, 1, MAX_WORKERS);
    cpu_set_t available;
    if (!sched_getaffinity(0, sizeof available, &available)) {
        unsigned count = (unsigned)CPU_COUNT(&available);
        if (count && count < workers) workers = count;
    }
    Pool pool = {0};
    init_pool(&pool, workers);
    Window window = manifest(data);
    init_keys();
    prepare_output(output);
    phase("startup");

    Catalog catalog = {0};
    Map aliases = {0};
    Timeline people[PERSON_SHARDS] = {0};
    build_sessions(data, &pool, &window, &catalog, &aliases, people);
    CampaignDays campaigns = attribute(data, output, &pool, &window, &catalog, &aliases, people);
    WriteJob write = {output, &catalog, people};
    if (!options.overlap) parallel(&pool, MAX_WORKERS, write_shard, &write);
    write_campaigns(output, &catalog, &campaigns);
    phase("final output");

    destroy_pool(&pool);
    free(campaigns.v);
    free(window.dates);
    destroy_map(&aliases);
    destroy_map(&catalog.products);
    destroy_map(&catalog.campaigns);
    free(catalog.categories);
    free(catalog.names);
    for (size_t i = 0; i < PERSON_SHARDS; ++i) {
        destroy_map(&people[i].index);
        free(people[i].sessions.v);
        free(people[i].days.v);
        free(people[i].people);
    }
    return 0;
}
