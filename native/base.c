#include "shopping.h"

#include <immintrin.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

Options options;

_Noreturn void fail(const char *format, ...) {
    va_list args;
    va_start(args, format);
    vfprintf(stderr, format, args);
    va_end(args);
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
}

void *resize_array(void *ptr, size_t n, size_t size) {
    if (n && size > SIZE_MAX / n) fail("allocation size overflow");
    void *result = realloc(ptr, n && size ? n * size : 1);
    if (!result) fail("cannot allocate %zu items of %zu bytes", n, size);
    return result;
}

void *allocate(size_t n, size_t size) { return resize_array(NULL, n, size); }

void *zero_allocate(size_t n, size_t size) {
    if (n && size > SIZE_MAX / n) fail("allocation size overflow");
    void *result = calloc(n ? n : 1, size ? size : 1);
    if (!result) fail("cannot allocate %zu items of %zu bytes", n, size);
    return result;
}

void *grow(void *ptr, size_t *capacity, size_t size) {
    if (*capacity > SIZE_MAX / 2) fail("capacity overflow");
    *capacity = *capacity ? *capacity * 2 : 16;
    return resize_array(ptr, *capacity, size);
}

char *path_join(const char *dir, const char *name) {
    size_t n = strlen(dir), m = strlen(name);
    if (n > SIZE_MAX - m - 2) fail("path length overflow");
    char *path = allocate(n + m + 2, 1);
    memcpy(path, dir, n);
    path[n] = '/';
    memcpy(path + n + 1, name, m + 1);
    return path;
}

static unsigned tag(uint64_t hash) { return (unsigned)(hash >> 57) + 1; }

static size_t grouped_slot(const Map *m, Key key, uint64_t hash) {
    size_t cap_mask = m->capacity - 1, at = hash & cap_mask;
    __m128i wanted = _mm_set1_epi8((char)tag(hash)), zero = _mm_setzero_si128();
    for (;;) {
        __m128i bytes = _mm_loadu_si128((const __m128i *)(m->control + at));
        unsigned possible = (unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(bytes, wanted));
        unsigned empty = (unsigned)_mm_movemask_epi8(_mm_cmpeq_epi8(bytes, zero));
        if (empty) possible &= empty ^ (empty - 1);
        while (possible) {
            size_t i = (at + (unsigned)__builtin_ctz(possible)) & cap_mask;
            if (m->keys[i] == key) return i;
            possible &= possible - 1;
        }
        if (empty) return (at + (unsigned)__builtin_ctz(empty)) & cap_mask;
        at = (at + 16) & cap_mask;
    }
}

uint64_t lookup_grouped(const Map *m, Key key, uint64_t hash, uint64_t missing) {
    size_t i = grouped_slot(m, key, hash);
    return m->keys[i] ? m->values[i] : missing;
}

static size_t map_slot(const Map *m, Key key, uint64_t hash) {
    if (m->control) return grouped_slot(m, key, hash);
    size_t at = hash & (m->capacity - 1);
    while (m->keys[at] && m->keys[at] != key) at = (at + 1) & (m->capacity - 1);
    return at;
}

static void store_tag(Map *m, size_t index, uint64_t hash) {
    if (!m->control) return;
    unsigned char value = (unsigned char)tag(hash);
    m->control[index] = value;
    if (index < 15) m->control[m->capacity + index] = value;
}

uint64_t *entry(Map *m, Key key, uint64_t initial) {
    if (!key) fail("empty key in exact table");
    if (m->used >= m->capacity - m->capacity / 4) {
        if (m->capacity > SIZE_MAX / 2) fail("map capacity overflow");
        size_t count = m->capacity ? m->capacity * 2 : 16;
        Map next = {
            .keys = zero_allocate(count, sizeof(Key)), .values = allocate(count, sizeof(uint64_t)),
            .control = options.grouped_map ? zero_allocate(count + 15, 1) : NULL,
            .capacity = count, .used = m->used,
        };
        for (size_t i = 0; i < m->capacity; ++i) {
            if (!m->keys[i]) continue;
            uint64_t hash = mix(m->keys[i]);
            size_t j = map_slot(&next, m->keys[i], hash);
            next.keys[j] = m->keys[i];
            next.values[j] = m->values[i];
            store_tag(&next, j, hash);
        }
        destroy_map(m);
        *m = next;
    }
    uint64_t hash = mix(key);
    size_t at = map_slot(m, key, hash);
    if (!m->keys[at]) {
        m->keys[at] = key;
        m->values[at] = initial;
        store_tag(m, at, hash);
        ++m->used;
    }
    return &m->values[at];
}

void destroy_map(Map *m) {
    free(m->keys);
    free(m->values);
    free(m->control);
    free(m->presence);
    *m = (Map){0};
}

void freeze_presence(Map *m) {
    if (!options.alias_filter || !m->used) return;
    size_t bits = 256;
    while (bits / 8 < m->used) {
        if (bits > SIZE_MAX / 2) fail("alias filter capacity overflow");
        bits *= 2;
    }
    m->presence = zero_allocate(bits / 64, sizeof(uint64_t));
    m->presence_mask = bits - 1;
    for (size_t i = 0; i < m->capacity; ++i) {
        if (!m->keys[i]) continue;
        size_t bit = mix(m->keys[i]) & m->presence_mask;
        m->presence[bit / 64] |= UINT64_C(1) << (bit % 64);
    }
}

static const unsigned char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-";
static unsigned char digit[256];
#define LONG_KEY (UINT64_C(1) << 63)
#define INTERN_SHARDS 64

typedef struct { uint64_t hash; size_t length; unsigned char text[]; } LongKey;
typedef struct { pthread_mutex_t lock; LongKey **slots; size_t capacity, used; } Interner;
static Interner interners[INTERN_SHARDS];

void init_keys(void) {
    memset(digit, 255, sizeof digit);
    for (unsigned i = 0; i < 64; ++i) digit[alphabet[i]] = (unsigned char)i;
    for (unsigned i = 0; i < INTERN_SHARDS; ++i) {
        if (pthread_mutex_init(&interners[i].lock, NULL)) fail("cannot initialize key interner");
    }
}

static Key intern_long(Slice text) {
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    for (size_t i = 0; i < text.n; ++i) hash = (hash ^ text.p[i]) * UINT64_C(0x100000001b3);
    hash = mix(hash);
    Interner *t = &interners[hash & (INTERN_SHARDS - 1)];
    if (pthread_mutex_lock(&t->lock)) fail("cannot lock key interner");
    if (t->used >= t->capacity - t->capacity / 4) {
        if (t->capacity > SIZE_MAX / 2) fail("interner capacity overflow");
        size_t count = t->capacity ? t->capacity * 2 : 32;
        LongKey **next = zero_allocate(count, sizeof *next);
        for (size_t i = 0; i < t->capacity; ++i) {
            LongKey *key = t->slots[i];
            if (!key) continue;
            size_t at = (key->hash >> 6) & (count - 1);
            while (next[at]) at = (at + 1) & (count - 1);
            next[at] = key;
        }
        free(t->slots);
        t->slots = next;
        t->capacity = count;
    }
    size_t at = (hash >> 6) & (t->capacity - 1);
    LongKey *key;
    while ((key = t->slots[at])) {
        if (key->hash == hash && key->length == text.n && !memcmp(key->text, text.p, text.n)) break;
        at = (at + 1) & (t->capacity - 1);
    }
    if (!key) {
        if (text.n > SIZE_MAX - sizeof *key) fail("key length overflow");
        key = allocate(sizeof *key + text.n, 1);
        key->hash = hash;
        key->length = text.n;
        memcpy(key->text, text.p, text.n);
        t->slots[at] = key;
        ++t->used;
    }
    if (pthread_mutex_unlock(&t->lock)) fail("cannot unlock key interner");
    uintptr_t bits = (uintptr_t)key;
    if (bits & LONG_KEY) fail("key arena address out of range");
    return (Key)bits | LONG_KEY;
}

Key intern(Slice text) {
    if (!text.n) return 0;
    if (options.compact_ids && text.n <= 10) {
        Key code = 1;
        for (size_t i = 0; i < text.n; ++i) {
            unsigned d = digit[text.p[i]];
            if (d == 255) return intern_long(text);
            code = (code << 6) | d;
        }
        return code;
    }
    return intern_long(text);
}

Slice key_bytes(Key key, unsigned char bytes[10]) {
    if (key & LONG_KEY) {
        const LongKey *long_key = (const LongKey *)(uintptr_t)(key & ~LONG_KEY);
        return (Slice){long_key->text, long_key->length};
    }
    size_t i = 10;
    while (key > 1) {
        bytes[--i] = alphabet[key & 63];
        key >>= 6;
    }
    return (Slice){bytes + i, 10 - i};
}

int compare_key(Key a, Key b) {
    if (a == b) return 0;
    unsigned char left[10], right[10];
    Slice x = key_bytes(a, left), y = key_bytes(b, right);
    int order = memcmp(x.p, y.p, x.n < y.n ? x.n : y.n);
    return order ? order : (x.n > y.n) - (x.n < y.n);
}

bool text_is(Slice text, const char *literal, size_t n) {
    return text.n == n && !memcmp(text.p, literal, n);
}

static int digits8(const unsigned char *p) {
    __m128i input = _mm_loadl_epi64((const __m128i *)p);
    __m128i low = _mm_cmplt_epi8(input, _mm_set1_epi8('0'));
    __m128i high = _mm_cmpgt_epi8(input, _mm_set1_epi8('9'));
    if (_mm_movemask_epi8(_mm_or_si128(low, high)) & 255) return -1;
    __m128i d = _mm_sub_epi8(input, _mm_set1_epi8('0'));
    __m128i pairs = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010a));
    __m128i quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x00010064));
    return _mm_cvtsi128_si32(quads) * 10000 + _mm_extract_epi32(quads, 1);
}

bool number(Slice text, int64_t limit, int64_t *result) {
    if (!text.n) return false;
    int64_t value = 0;
    size_t i = 0;
    while (options.digits_simd && text.n - i >= 8) {
        int block = digits8(text.p + i);
        if (block < 0 || block > limit || value > (limit - block) / INT64_C(100000000)) return false;
        value = value * INT64_C(100000000) + block;
        i += 8;
    }
    for (; i < text.n; ++i) {
        unsigned d = (unsigned)text.p[i] - '0';
        if (d > 9 || (int64_t)d > limit || value > (limit - d) / 10) return false;
        value = value * 10 + d;
    }
    *result = value;
    return true;
}

static bool leap(int y) { return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0); }

static int month_days(int year, int month) {
    static const int lengths[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return lengths[month] + (month == 2 && leap(year));
}

static int32_t civil_day(int year, int month, int day) {
    year -= month <= 2;
    int era = (year >= 0 ? year : year - 399) / 400;
    int y = year - 400 * era;
    int m = month > 2 ? month - 3 : month + 9;
    return 146097 * era + y * 365 + y / 4 - y / 100 + (153 * m + 2) / 5 + day - 1 - 719468;
}

int32_t utc_day(int64_t ms) {
    return (int32_t)(ms / DAY_MS - (ms % DAY_MS < 0));
}

void init_calendar(Window *w, int first, int last) {
    if (!options.calendar || first < 0 || last > 9999 || last < first || last - first > 8) return;
    w->first_year = first;
    w->years = last - first + 1;
    size_t count = (size_t)w->years * 512;
    w->dates = allocate(count, sizeof *w->dates);
    for (size_t i = 0; i < count; ++i) w->dates[i] = INT32_MIN;
    for (int y = first; y <= last; ++y) {
        for (int m = 1; m <= 12; ++m) {
            for (int d = 1; d <= month_days(y, m); ++d)
                w->dates[(y - first) * 512 + m * 32 + d] = civil_day(y, m, d);
        }
    }
}

static int pair(const unsigned char *p) {
    unsigned a = (unsigned)p[0] - '0', b = (unsigned)p[1] - '0';
    return (a <= 9 && b <= 9) ? (int)(a * 10 + b) : -1;
}

static bool iso_digits(const unsigned char *p, uint16_t result[8]) {
    __m128i left = _mm_loadu_si128((const __m128i *)p);
    __m128i right = _mm_loadl_epi64((const __m128i *)(p + 12));
    __m128i a = _mm_shuffle_epi8(left, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
    __m128i b = _mm_shuffle_epi8(right, _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 5, 6, -1, -1));
    __m128i joined = _mm_or_si128(a, b);
    __m128i low = _mm_cmplt_epi8(joined, _mm_set1_epi8('0'));
    __m128i high = _mm_cmpgt_epi8(joined, _mm_set1_epi8('9'));
    if (_mm_movemask_epi8(_mm_or_si128(low, high)) & 0x3fff) return false;
    __m128i values = _mm_sub_epi8(joined, _mm_set1_epi8('0'));
    _mm_storeu_si128((__m128i *)result, _mm_maddubs_epi16(values, _mm_set1_epi16(0x010a)));
    return true;
}

bool timestamp(Slice text, const Window *window, bool ingest, int64_t *result) {
    const unsigned char *p = text.p;
    if ((text.n != 20 && text.n != 25) || p[10] != 'T') {
        return !ingest && number(text, INT64_MAX, result);
    }
    bool trusted = ingest && text.n == 20 && p[19] == 'Z';
    if (!trusted && (p[4] != '-' || p[7] != '-' || p[13] != ':' || p[16] != ':')) return false;
    int yh, yl, m, d, h, mm, s;
    if (options.digits_simd) {
        uint16_t parts[8];
        if (!iso_digits(p, parts)) return false;
        yh = parts[0]; yl = parts[1]; m = parts[2]; d = parts[3];
        h = parts[4]; mm = parts[5]; s = parts[6];
    } else {
        yh = pair(p); yl = pair(p + 2); m = pair(p + 5); d = pair(p + 8);
        h = pair(p + 11); mm = pair(p + 14); s = pair(p + 17);
    }
    if (yh < 0 || yl < 0 || m < 1 || m > 12 || d < 1 || d > 31 || h < 0 || h > 23 || mm < 0 || mm > 59 || s < 0 || s > 59) return false;
    int y = yh * 100 + yl;
    int32_t ordinal;
    if (window && window->dates && (unsigned)(y - window->first_year) < (unsigned)window->years) {
        ordinal = window->dates[(y - window->first_year) * 512 + m * 32 + d];
        if (ordinal == INT32_MIN) return false;
    } else {
        if (d > month_days(y, m)) return false;
        ordinal = civil_day(y, m, d);
    }
    int offset = 0;
    if (text.n == 25 && !ingest) {
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
        int oh = pair(p + 20), om = pair(p + 23);
        if (oh < 0 || oh > 23 || om < 0 || om > 59) return false;
        offset = oh * 60 + om;
        if (p[19] == '-') offset = -offset;
    } else if (text.n != 20 || p[19] != 'Z') {
        return false;
    }
    *result = (int64_t)ordinal * DAY_MS + ((h * 60 + mm - offset) * 60 + s) * INT64_C(1000);
    return true;
}

typedef struct { Pool *pool; unsigned worker; } ThreadStart;

static void run_work(Pool *p, unsigned worker) {
    if (options.static_jobs) {
        for (size_t i = worker; i < p->work; i += p->workers) p->task(p->data, i, worker);
        return;
    }
    size_t i;
    while ((i = atomic_fetch_add_explicit(&p->next, 1, memory_order_relaxed)) < p->work)
        p->task(p->data, i, worker);
}

static void *thread_main(void *data) {
    ThreadStart start = *(ThreadStart *)data;
    free(data);
    Pool *p = start.pool;
    for (;;) {
        pthread_barrier_wait(&p->begin);
        if (!p->task) return NULL;
        run_work(p, start.worker);
        pthread_barrier_wait(&p->end);
    }
}

void init_pool(Pool *p, unsigned workers) {
    p->workers = workers;
    atomic_init(&p->next, 0);
    if (pthread_barrier_init(&p->begin, NULL, workers) || pthread_barrier_init(&p->end, NULL, workers))
        fail("cannot initialize worker barriers");
    for (unsigned i = 1; i < workers; ++i) {
        ThreadStart *start = allocate(1, sizeof *start);
        *start = (ThreadStart){p, i};
        if (pthread_create(&p->threads[i - 1], NULL, thread_main, start)) fail("cannot create worker");
    }
}

void parallel(Pool *p, size_t jobs, Task task, void *data) {
    p->task = task;
    p->data = data;
    p->work = jobs;
    atomic_store_explicit(&p->next, 0, memory_order_relaxed);
    pthread_barrier_wait(&p->begin);
    run_work(p, 0);
    pthread_barrier_wait(&p->end);
}

void destroy_pool(Pool *p) {
    p->task = NULL;
    pthread_barrier_wait(&p->begin);
    for (unsigned i = 1; i < p->workers; ++i) pthread_join(p->threads[i - 1], NULL);
    pthread_barrier_destroy(&p->begin);
    pthread_barrier_destroy(&p->end);
}

void phase(const char *label) {
    static struct timespec previous;
    if (!options.profile) return;
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now)) fail("clock_gettime failed");
    if (label) fprintf(stderr, "%s %.6fs\n", label, (double)(now.tv_sec - previous.tv_sec) + (double)(now.tv_nsec - previous.tv_nsec) / 1e9);
    previous = now;
}
