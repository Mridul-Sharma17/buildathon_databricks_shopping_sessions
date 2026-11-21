#include "shopping.h"

#include <immintrin.h>
#include <string.h>

typedef struct { Key id; int64_t ts; uint32_t person, campaign, first, end; } Click;
VECTOR(Click, Clicks);
VECTOR(uint64_t, Words);
VECTOR(size_t, Cells);
typedef struct { Key id; int64_t ts; uint32_t campaign; } Best;
typedef struct { size_t mask_start; uint32_t first, end; } Envelope;
VECTOR(Envelope, Envelopes);
typedef struct {
    Map index, envelope_index;
    Words masks;
    Envelopes envelopes;
    Best *winners;
    unsigned char *has_clicks;
} Coverage;
typedef struct { int32_t day; uint32_t word; uint64_t mask; } DayMask;
VECTOR(DayMask, DayMasks);

typedef struct {
    Metric *dense;
    Cells touched;
    Map sparse;
    CampaignDays rows;
    int32_t base;
    size_t days, campaigns;
} Totals;

static Totals new_totals(const Window *window, size_t campaigns) {
    Totals total = {.base = utc_day(window->start), .campaigns = campaigns};
    total.days = (size_t)(utc_day(window->end - 1) - total.base + 1);
    if (campaigns <= 65536 / total.days)
        total.dense = zero_allocate(campaigns * total.days, sizeof *total.dense);
    return total;
}

static Metric *cell(Totals *t, uint32_t campaign, int32_t day) {
    if (t->dense) {
        size_t i = campaign * t->days + (size_t)(day - t->base);
        Metric *record = &t->dense[i];
        if (!record->sessions && !record->assists) PUSH(&t->touched, i);
        return record;
    }
    Key key = ((uint64_t)campaign << 32) | ((uint32_t)day ^ UINT32_C(0x80000000));
    size_t row = *entry(&t->sparse, key, t->rows.n);
    if (row == t->rows.n) PUSH(&t->rows, ((CampaignDay){.campaign = campaign, .day = day}));
    return &t->rows.v[row].values;
}

static void materialize(Totals *t) {
    for (size_t i = 0; i < t->touched.n; ++i) {
        size_t key = t->touched.v[i];
        PUSH(&t->rows, ((CampaignDay){(uint32_t)(key / t->days), t->base + (int32_t)(key % t->days), t->dense[key]}));
    }
    free(t->dense);
    free(t->touched.v);
    destroy_map(&t->sparse);
}

static uint64_t mask_in_word(size_t word, size_t first, size_t end) {
    size_t base = word * 64;
    unsigned lo = first > base ? (unsigned)(first - base) : 0;
    unsigned hi = end - base < 64 ? (unsigned)(end - base) : 64;
    return (UINT64_MAX << lo) & (UINT64_MAX >> (64 - hi));
}

static size_t bound(const Session *s, size_t n, int64_t time, bool inclusive) {
    size_t lo = 0, hi = n;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (s[mid].start < time || (inclusive && s[mid].start == time)) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

typedef struct {
    Input input;
    const char *output;
    const Window *window;
    const Catalog *catalog;
    const Map *aliases;
    const Timeline *people;
    unsigned workers;
    size_t base, scan_jobs;
    Clicks *inbox;
    Map routes;
    Coverage coverage[PERSON_SHARDS];
    Totals totals[MAX_WORKERS];
} Job;

static void collect_click(void *data, unsigned worker, Slice row, const Slice *f) {
    (void)row;
    Job *job = data;
    if (!f[0].n || !f[1].n || !f[2].n) return;
    Key user = intern(f[1]);
    uint64_t id;
    size_t shard;
    if (options.alias_route) {
        uint64_t route = lookup(&job->routes, user, UINT64_MAX);
        if (route == UINT64_MAX) return;
        shard = route >> 32;
        id = (uint32_t)route;
    } else {
        Key person = lookup(job->aliases, user, user);
        shard = mix(person) & (PERSON_SHARDS - 1);
        id = lookup(&job->people[shard].index, person, UINT64_MAX);
    }
    if (id == UINT64_MAX) return;
    uint64_t campaign = lookup(&job->catalog->campaigns, intern(f[2]), 0);
    if (!campaign) return;
    int64_t ts;
    if (!timestamp(f[3], job->window, false, &ts) || ts < job->window->start - WEEK_MS || ts >= job->window->end) return;
    const Timeline *t = &job->people[shard];
    Person p = t->people[id];
    const Session *starts = t->sessions.v + p.begin;
    size_t n = p.end - p.begin, first = 0, end = 0;
    int64_t expiry = ts + WEEK_MS;
    if (options.click == CLICK_SWEEP) {
        first = 0;
        end = n;
    } else if (n <= options.tiny) {
        size_t i = 0;
        if (options.tiny_simd) {
            __m256i begin = _mm256_set1_epi64x(ts), stop = _mm256_set1_epi64x(expiry);
            for (; n - i >= 4; i += 4) {
                __m256i dates = _mm256_setr_epi64x(starts[i].start, starts[i + 1].start, starts[i + 2].start, starts[i + 3].start);
                first += (unsigned)__builtin_popcount((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(begin, dates))));
                end += 4u - (unsigned)__builtin_popcount((unsigned)_mm256_movemask_pd(_mm256_castsi256_pd(_mm256_cmpgt_epi64(dates, stop))));
            }
        }
        for (; i < n; ++i) {
            first += starts[i].start < ts;
            end += starts[i].start <= expiry;
        }
    } else {
        first = bound(starts, n, ts, false);
        end = first + bound(starts + first, n - first, expiry, true);
    }
    if (first == end) return;
    if (id > UINT32_MAX || end > UINT32_MAX) fail("person or session index out of range");
    PUSH(&job->inbox[worker * PERSON_SHARDS + shard],
        ((Click){intern(f[0]), ts, (uint32_t)id, (uint32_t)campaign, (uint32_t)first, (uint32_t)end}));
}

static void route_table(Job *job) {
    for (size_t s = 0; s < PERSON_SHARDS; ++s) {
        const Map *index = &job->people[s].index;
        for (size_t i = 0; i < index->capacity; ++i) {
            if (!index->keys[i]) continue;
            if (index->values[i] > UINT32_MAX) fail("person index out of range");
            *entry(&job->routes, index->keys[i], 0) = ((uint64_t)s << 32) | index->values[i];
        }
    }
    for (size_t i = 0; i < job->aliases->capacity; ++i) {
        if (!job->aliases->keys[i]) continue;
        uint64_t route = lookup(&job->routes, job->aliases->values[i], UINT64_MAX);
        if (route != UINT64_MAX) *entry(&job->routes, job->aliases->keys[i], route) = route;
    }
}

static void scan_clicks(void *data, size_t i, unsigned worker) {
    Job *job = data;
    if (i >= job->scan_jobs) {
        unsigned part = (unsigned)(i - job->scan_jobs);
        size_t per_file = PERSON_SHARDS / MAX_WORKERS;
        write_users(job->output, job->catalog, job->people, part * per_file, (part + 1) * per_file, part);
    } else scan_range(&job->input.ranges[job->base + i], 4, collect_click, job, worker);
}

static void add_mask(Coverage *c, size_t start, size_t first, size_t end) {
    for (size_t word = first / 64; word <= (end - 1) / 64; ++word)
        c->masks.v[start + word] |= mask_in_word(word, first, end);
}

static void apply_click(const Timeline *t, Coverage *c, Click click, const Window *window) {
    Person p = t->people[click.person];
    if (!c->winners) {
        c->winners = zero_allocate(t->sessions.n, sizeof *c->winners);
        c->has_clicks = zero_allocate(t->count, 1);
    }
    c->has_clicks[click.person] = 1;
    Best *best = &c->winners[p.begin + click.first];
    if (!best->id || click.ts > best->ts || (click.ts == best->ts && compare_key(click.id, best->id) < 0))
        *best = (Best){click.id, click.ts, click.campaign};

    Key key = ((uint64_t)click.person << 32) | click.campaign;
    size_t start = *entry(&c->index, key, c->masks.n);
    if (start == c->masks.n) {
        size_t words = (p.end - p.begin + 63) / 64;
        if (words > SIZE_MAX - start) fail("assist mask capacity overflow");
        size_t end = start + words;
        while (c->masks.cap < end)
            c->masks.v = grow(c->masks.v, &c->masks.cap, sizeof *c->masks.v);
        memset(c->masks.v + start, 0, words * sizeof *c->masks.v);
        c->masks.n = end;
    }
    if (options.click == CLICK_ENVELOPE) {
        uint64_t weeks = (uint64_t)((window->end - window->start - 1) / WEEK_MS + 2);
        uint64_t bin = (uint64_t)((click.ts - (window->start - WEEK_MS)) / WEEK_MS);
        if (start > (UINT64_MAX - bin - 1) / weeks) fail("click envelope index overflow");
        Key key = start * weeks + bin + 1;
        size_t entry_index = *entry(&c->envelope_index, key, c->envelopes.n);
        if (entry_index == c->envelopes.n) PUSH(&c->envelopes, ((Envelope){start, click.first, click.end}));
        else {
            Envelope *e = &c->envelopes.v[entry_index];
            if (click.first < e->first) e->first = click.first;
            if (click.end > e->end) e->end = click.end;
        }
    } else if (p.end - p.begin <= 64) {
        c->masks.v[start] |= mask_in_word(0, click.first, click.end);
    } else add_mask(c, start, click.first, click.end);
}

static int order_clicks(const void *a, const void *b) {
    const Click *x = a, *y = b;
    if (x->person != y->person) return (x->person > y->person) - (x->person < y->person);
    return (x->ts > y->ts) - (x->ts < y->ts);
}

static void reduce_inbox(void *data, size_t shard, unsigned worker) {
    (void)worker;
    Job *job = data;
    const Timeline *t = &job->people[shard];
    Coverage *c = &job->coverage[shard];
    Clicks ordered = {0};
    for (unsigned w = 0; w < job->workers; ++w) {
        Clicks *inbox = &job->inbox[w * PERSON_SHARDS + shard];
        for (size_t i = 0; i < inbox->n; ++i) {
            Click click = inbox->v[i];
            if (options.click == CLICK_SWEEP) PUSH(&ordered, click);
            else apply_click(t, c, click, job->window);
        }
        inbox->n = 0;
    }
    if (ordered.n > 1) qsort(ordered.v, ordered.n, sizeof *ordered.v, order_clicks);
    uint32_t active = UINT32_MAX;
    size_t first = 0, end = 0;
    for (size_t i = 0; i < ordered.n; ++i) {
        Click click = ordered.v[i];
        Person p = t->people[click.person];
        if (active != click.person) { first = end = p.begin; active = click.person; }
        while (first < p.end && t->sessions.v[first].start < click.ts) ++first;
        if (end < first) end = first;
        while (end < p.end && t->sessions.v[end].start - click.ts <= WEEK_MS) ++end;
        if (first == end) continue;
        click.first = (uint32_t)(first - p.begin);
        click.end = (uint32_t)(end - p.begin);
        apply_click(t, c, click, job->window);
    }
    free(ordered.v);
}

static void count_credits(const Timeline *t, const Coverage *c, Totals *total, size_t person) {
    Person p = t->people[person];
    if (!c->has_clicks || !c->has_clicks[person]) {
        for (size_t i = p.begin; i < p.end;) {
            int32_t day = utc_day(t->sessions.v[i].start);
            Metric group = {0};
            do {
                const Session *s = &t->sessions.v[i];
                ++group.sessions;
                group.purchasing += s->revenue > 0;
                group.revenue += s->revenue;
            } while (++i < p.end && utc_day(t->sessions.v[i].start) == day);
            Metric *m = cell(total, 0, day);
            m->sessions += group.sessions;
            m->purchasing += group.purchasing;
            m->revenue += group.revenue;
        }
        return;
    }
    Best recent = {0};
    for (size_t i = p.begin; i < p.end; ++i) {
        if (c->winners[i].id) recent = c->winners[i];
        const Session *s = &t->sessions.v[i];
        uint32_t campaign = recent.id && s->start - recent.ts <= WEEK_MS ? recent.campaign : 0;
        Metric *m = cell(total, campaign, utc_day(s->start));
        ++m->sessions;
        m->purchasing += s->revenue > 0;
        m->revenue += s->revenue;
    }
}

static void summarize(void *data, size_t shard, unsigned worker) {
    Job *job = data;
    const Timeline *t = &job->people[shard];
    Coverage *c = &job->coverage[shard];
    for (size_t i = 0; i < c->envelopes.n; ++i) {
        Envelope e = c->envelopes.v[i];
        add_mask(c, e.mask_start, e.first, e.end);
    }
    free(c->envelopes.v);
    destroy_map(&c->envelope_index);
    Totals *total = &job->totals[worker];
    DayMasks masks = {0};
    size_t *bounds = options.mask_days && c->has_clicks ? allocate(t->count + 1, sizeof *bounds) : NULL;
    for (size_t person = 0; person < t->count; ++person) {
        count_credits(t, c, total, person);
        if (!bounds) continue;
        bounds[person] = masks.n;
        if (!c->has_clicks[person]) continue;
        Person p = t->people[person];
        for (size_t i = p.begin; i < p.end;) {
            size_t first = i - p.begin;
            int32_t day = utc_day(t->sessions.v[i].start);
            do ++i;
            while (i < p.end && utc_day(t->sessions.v[i].start) == day);
            size_t end = i - p.begin;
            for (size_t word = first / 64; word <= (end - 1) / 64; ++word)
                PUSH(&masks, ((DayMask){day, (uint32_t)word, mask_in_word(word, first, end)}));
        }
    }
    if (bounds) bounds[t->count] = masks.n;
    for (size_t i = 0; i < c->index.capacity; ++i) {
        Key key = c->index.keys[i];
        if (!key) continue;
        uint32_t campaign = (uint32_t)key, person = (uint32_t)(key >> 32);
        size_t mask_start = c->index.values[i];
        if (bounds) {
            for (size_t j = bounds[person]; j < bounds[person + 1]; ++j) {
                DayMask day = masks.v[j];
                int count = __builtin_popcountll(c->masks.v[mask_start + day.word] & day.mask);
                if (count) cell(total, campaign, day.day)->assists += count;
            }
            continue;
        }
        Person p = t->people[person];
        for (size_t j = p.begin; j < p.end;) {
            size_t first = j - p.begin;
            int32_t day = utc_day(t->sessions.v[j].start);
            do ++j;
            while (j < p.end && utc_day(t->sessions.v[j].start) == day);
            size_t end = j - p.begin;
            int64_t count = 0;
            for (size_t word = first / 64; word <= (end - 1) / 64; ++word)
                count += __builtin_popcountll(c->masks.v[mask_start + word] & mask_in_word(word, first, end));
            if (count) cell(total, campaign, day)->assists += count;
        }
    }
    free(bounds);
    free(masks.v);
    free(c->winners);
    free(c->has_clicks);
    free(c->masks.v);
    destroy_map(&c->index);
}

CampaignDays attribute(const char *data, const char *output, Pool *pool, const Window *window, const Catalog *catalog, const Map *aliases, const Timeline *people) {
    char *path = path_join(data, "clicks");
    Job job = {
        .input = open_input(path, NULL), .output = output, .window = window, .catalog = catalog,
        .aliases = aliases, .people = people, .workers = pool->workers,
        .inbox = zero_allocate(pool->workers * PERSON_SHARDS, sizeof(Clicks)),
    };
    free(path);
    load_input(&job.input, pool);
    if (options.alias_route) route_table(&job);
    size_t batch = options.click_batch ? options.click_batch : job.input.nranges;
    bool write_now = options.overlap;
    for (size_t base = 0; base < job.input.nranges || write_now;) {
        job.base = base;
        job.scan_jobs = job.input.nranges - base;
        if (job.scan_jobs > batch) job.scan_jobs = batch;
        parallel(pool, job.scan_jobs + (write_now ? MAX_WORKERS : 0), scan_clicks, &job);
        write_now = false;
        if (job.scan_jobs) parallel(pool, PERSON_SHARDS, reduce_inbox, &job);
        base += job.scan_jobs;
    }
    close_input(&job.input);
    destroy_map(&job.routes);
    for (size_t i = 0; i < pool->workers * PERSON_SHARDS; ++i) free(job.inbox[i].v);
    free(job.inbox);
    phase("click scan and campaign coverage");
    for (unsigned w = 0; w < pool->workers; ++w) job.totals[w] = new_totals(window, catalog->campaign_count);
    parallel(pool, PERSON_SHARDS, summarize, &job);
    Totals reduced = new_totals(window, catalog->campaign_count);
    for (unsigned w = 0; w < pool->workers; ++w) {
        Totals *worker = &job.totals[w];
        materialize(worker);
        for (size_t i = 0; i < worker->rows.n; ++i) {
            const CampaignDay *row = &worker->rows.v[i];
            Metric *m = cell(&reduced, row->campaign, row->day);
            m->sessions += row->values.sessions;
            m->purchasing += row->values.purchasing;
            m->revenue += row->values.revenue;
            m->assists += row->values.assists;
        }
        free(worker->rows.v);
    }
    materialize(&reduced);
    phase("credit and touched campaign totals");
    return reduced.rows;
}
