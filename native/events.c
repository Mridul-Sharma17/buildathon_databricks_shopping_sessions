#include "shopping.h"

#include <string.h>

typedef struct { uint32_t parent, size; Key name; } Node;
VECTOR(Node, Nodes);
VECTOR(Key, Keys);
VECTOR(Campaign, Campaigns);
typedef struct { Map index; Nodes nodes; } Links;

static Key literal(const char *s) { return intern((Slice){(const unsigned char *)s, strlen(s)}); }

static int order_keys(const void *a, const void *b) { return compare_key(*(const Key *)a, *(const Key *)b); }

static uint32_t root(Nodes *nodes, uint32_t id) {
    while (id != nodes->v[id].parent) {
        uint32_t parent = nodes->v[id].parent;
        nodes->v[id].parent = nodes->v[parent].parent;
        id = nodes->v[id].parent;
    }
    return id;
}

static uint32_t node(Links *links, Key key) {
    size_t n = links->nodes.n;
    if (n >= UINT32_MAX) fail("too many linked IDs");
    uint64_t id = *entry(&links->index, key, n);
    if (id == n) PUSH(&links->nodes, ((Node){(uint32_t)n, 1, key}));
    return (uint32_t)id;
}

static void add_link(void *ctx, unsigned worker, Slice row, const Slice *f) {
    (void)worker;
    (void)row;
    if (!f[0].n || !f[1].n) return;
    Links *links = ctx;
    uint32_t left = root(&links->nodes, node(links, intern(f[0])));
    uint32_t right = root(&links->nodes, node(links, intern(f[1])));
    if (left == right) return;
    if (links->nodes.v[left].size < links->nodes.v[right].size) {
        uint32_t swap = left;
        left = right;
        right = swap;
    }
    links->nodes.v[right].parent = left;
    links->nodes.v[left].size += links->nodes.v[right].size;
}

static void aliases_from(const Input *input, Map *aliases) {
    Links links = {0};
    for (size_t i = 0; i < input->nranges; ++i) scan_range(&input->ranges[i], 2, add_link, &links, 0);
    Key *minima = zero_allocate(links.nodes.n, sizeof *minima);
    for (size_t i = 0; i < links.nodes.n; ++i) {
        uint32_t component = root(&links.nodes, (uint32_t)i);
        Node *n = &links.nodes.v[i];
        n->parent = component;
        if (!minima[component] || compare_key(n->name, minima[component]) < 0) minima[component] = n->name;
    }
    for (size_t i = 0; i < links.nodes.n; ++i) {
        Node n = links.nodes.v[i];
        Key canonical = minima[n.parent];
        if (n.name != canonical) *entry(aliases, n.name, canonical) = canonical;
    }
    free(minima);
    free(links.nodes.v);
    destroy_map(&links.index);
    freeze_presence(aliases);
}

typedef struct { Catalog *catalog; Keys names; Map seen; } Products;

static void add_product(void *ctx, unsigned worker, Slice row, const Slice *f) {
    (void)worker;
    (void)row;
    Products *p = ctx;
    Key name = intern(f[0]), category = intern(f[1]);
    if (!name) return;
    if (!category) category = literal("unknown");
    if (*entry(&p->seen, category, 0) == 0) {
        *entry(&p->seen, category, 1) = 1;
        PUSH(&p->names, category);
    }
    *entry(&p->catalog->products, name, category) = category;
}

static void products_from(const Input *input, Catalog *catalog) {
    Products p = {.catalog = catalog};
    Key unknown = literal("unknown");
    *entry(&p.seen, unknown, 1) = 1;
    PUSH(&p.names, unknown);
    for (size_t i = 0; i < input->nranges; ++i) scan_range(&input->ranges[i], 2, add_product, &p, 0);
    if (p.names.n >= NO_CATEGORY) fail("too many categories");
    qsort(p.names.v, p.names.n, sizeof *p.names.v, order_keys);
    for (size_t i = 0; i < p.names.n; ++i) *entry(&p.seen, p.names.v[i], i) = i;
    for (size_t i = 0; i < catalog->products.capacity; ++i) {
        if (!catalog->products.keys[i]) continue;
        catalog->products.values[i] = lookup(&p.seen, catalog->products.values[i], NO_CATEGORY);
    }
    catalog->categories = p.names.v;
    catalog->category_count = p.names.n;
    catalog->unknown = (uint32_t)lookup(&p.seen, unknown, NO_CATEGORY);
    destroy_map(&p.seen);
}

typedef struct { Catalog *catalog; Campaigns names; } CampaignBuild;

static void add_campaign(void *ctx, unsigned worker, Slice row, const Slice *f) {
    (void)worker;
    (void)row;
    CampaignBuild *b = ctx;
    Key id = intern(f[0]);
    if (!id) return;
    if (b->names.n >= UINT32_MAX) fail("too many campaigns");
    *entry(&b->catalog->campaigns, id, b->names.n) = b->names.n;
    PUSH(&b->names, ((Campaign){id, intern(f[1])}));
}

static void campaigns_from(const Input *input, Catalog *catalog) {
    CampaignBuild b = {.catalog = catalog};
    Key organic = literal("organic");
    PUSH(&b.names, ((Campaign){organic, organic}));
    for (size_t i = 0; i < input->nranges; ++i) scan_range(&input->ranges[i], 2, add_campaign, &b, 0);
    catalog->names = b.names.v;
    catalog->campaign_count = b.names.n;
}

typedef struct { Key id; const unsigned char *data; uint64_t fields; } Copy;
#define LONG_ROW (UINT64_C(1) << 63)
typedef struct { int64_t ts; uint32_t cents, category; } Event;
typedef struct { Key person; Event event; } Routed;
typedef struct { Key person; uint64_t event; } PackedRouted;
typedef struct { Key id, user; int64_t ingest; uint64_t event; } Projected;
VECTOR(Copy, Copies);
VECTOR(Routed, RoutedEvents);
VECTOR(PackedRouted, PackedEvents);
VECTOR(Projected, Projections);

typedef struct {
    Copies copies[EVENT_SHARDS];
    RoutedEvents people[PERSON_SHARDS];
    PackedEvents packed_people[PERSON_SHARDS];
    Projections projected[EVENT_SHARDS];
    uint64_t *seen, *repeated;
} EventWorker;

typedef struct {
    Input events, products, campaigns, links;
    EventWorker *workers;
    unsigned nworkers;
    const Window *window;
    Catalog *catalog;
    Map *aliases;
    Timeline *timelines;
    size_t slots;
    uint64_t *seen, *repeated;
    bool packed, eager;
} Build;

static size_t round_power(size_t n) {
    size_t p = 1;
    while (p < n) {
        if (p > SIZE_MAX / 2) fail("filter size overflow");
        p *= 2;
    }
    return p;
}

static Copy capture(Key id, Slice row, const Slice *fields) {
    if (row.n > 255 || !options.cache_fields) return (Copy){id, row.p, LONG_ROW | row.n};
    uint64_t packed = 0;
    const unsigned char *start = row.p;
    for (unsigned i = 0; i < 7; ++i) {
        bool quoted = fields[i].p != start;
        const unsigned char *end = fields[i].p + fields[i].n + quoted;
        packed |= (uint64_t)(end - row.p) << (8 * i);
        packed |= (uint64_t)quoted << (56 + i);
        if (i != 6) start = end + 1;
    }
    return (Copy){id, row.p, packed};
}

static inline Slice saved_field(const Copy *c, unsigned i) {
    size_t start = i ? ((c->fields >> (8 * (i - 1))) & 255) + 1 : 0;
    size_t end = (c->fields >> (8 * i)) & 255;
    size_t quoted = (c->fields >> (56 + i)) & 1;
    return (Slice){c->data + start + quoted, end - start - 2 * quoted};
}

static void exact_fields(const Copy *c, Slice f[7]) {
    if (c->fields & LONG_ROW) {
        if (!parse_fields((Slice){c->data, c->fields & ~LONG_ROW}, f, 7)) fail("invalid stored event boundary");
    } else {
        for (unsigned i = 0; i < 7; ++i) f[i] = saved_field(c, i);
    }
}

static bool project_event(Build *b, const Slice *f, int64_t ingest, Event *event, Key *user) {
    if (!f[1].n || !f[5].n) return false;
    bool purchase = text_is(f[4], "purchase", 8);
    if (!purchase && !text_is(f[4], "view", 4) && !text_is(f[4], "cart", 4)) return false;
    int64_t ts;
    if (!timestamp(f[2], b->window, false, &ts) || ts < b->window->start || ts >= b->window->end) return false;
    if (ingest == INT64_MIN && !timestamp(f[3], b->window, true, &ingest)) fail("invalid ingestion timestamp in winner");
    if (ingest - ts > DAY_MS) return false;
    int64_t cents = 0;
    uint32_t category = NO_CATEGORY;
    if (purchase) {
        if (!number(f[6], 10000000, &cents) || !cents) return false;
        category = (uint32_t)lookup(&b->catalog->products, intern(f[5]), b->catalog->unknown);
    }
    *user = intern(f[1]);
    *event = (Event){ts, (uint32_t)cents, category};
    return true;
}

static uint64_t pack_event(const Window *window, Event event) {
    return ((uint64_t)(event.ts - window->start) << 32) |
        ((uint64_t)(event.cents ? event.category : 0) << 24) | event.cents;
}

static void emit_event(Build *b, unsigned worker, const Slice *f) {
    Event event;
    Key user;
    if (!project_event(b, f, INT64_MIN, &event, &user)) return;
    Key person = lookup(b->aliases, user, user);
    size_t shard = mix(person) & (PERSON_SHARDS - 1);
    if (b->packed) {
        PUSH(&b->workers[worker].packed_people[shard], ((PackedRouted){person, pack_event(b->window, event)}));
    } else PUSH(&b->workers[worker].people[shard], ((Routed){person, event}));
}

static void collect_projected(void *data, unsigned worker, Slice row, const Slice *f) {
    (void)row;
    Build *b = data;
    if (!f[0].n) return;
    Projected copy = {.id = intern(f[0])};
    if (!timestamp(f[3], b->window, true, &copy.ingest)) fail("invalid ingestion timestamp in projected copy");
    Event event;
    if (project_event(b, f, copy.ingest, &event, &copy.user)) copy.event = pack_event(b->window, event);
    PUSH(&b->workers[worker].projected[mix(copy.id) & (EVENT_SHARDS - 1)], copy);
}

static void collect_copy(void *data, unsigned worker, Slice row, const Slice *f) {
    Build *b = data;
    if (!f[0].n) return;
    Key id = intern(f[0]);
    uint64_t hash = mix(id);
    if (options.dedup == DEDUP_TWO_SCAN) {
        size_t slot = hash & (b->slots - 1), word = slot / 64;
        uint64_t bit = UINT64_C(1) << (slot % 64);
        if ((b->seen[word] & bit) && !(b->repeated[word] & bit)) {
            emit_event(b, worker, f);
            return;
        }
    }
    PUSH(&b->workers[worker].copies[hash & (EVENT_SHARDS - 1)], capture(id, row, f));
}

static void count_copy(void *data, unsigned worker, Slice row, const Slice *f) {
    (void)row;
    Build *b = data;
    if (!f[0].n) return;
    size_t slot = mix(intern(f[0])) & (b->slots - 1), word = slot / 64;
    uint64_t bit = UINT64_C(1) << (slot % 64);
    b->workers[worker].repeated[word] |= b->workers[worker].seen[word] & bit;
    b->workers[worker].seen[word] |= bit;
}

static void prepare(void *data, size_t job, unsigned worker) {
    Build *b = data;
    switch (job) {
    case 0: products_from(&b->products, b->catalog); break;
    case 1: campaigns_from(&b->campaigns, b->catalog); break;
    case 2: aliases_from(&b->links, b->aliases); break;
    default:
        scan_range(&b->events.ranges[job - 3], 7,
            options.dedup == DEDUP_TWO_SCAN ? count_copy : collect_copy, b, worker);
        break;
    }
}

static void prepare_eager(void *data, size_t job, unsigned worker) {
    Build *b = data;
    if (!job) aliases_from(&b->links, b->aliases);
    else scan_range(&b->events.ranges[job - 1], 7, b->eager ? collect_projected : collect_copy, b, worker);
}

static void scan_copies(void *data, size_t job, unsigned worker) {
    Build *b = data;
    scan_range(&b->events.ranges[job], 7, collect_copy, b, worker);
}

static void merge_counts(void *data, size_t job, unsigned worker) {
    (void)worker;
    Build *b = data;
    size_t start = job * 4096, end = start + 4096;
    if (end > b->slots / 64) end = b->slots / 64;
    for (size_t i = start; i < end; ++i) {
        uint64_t seen = 0, repeated = 0;
        for (unsigned w = 0; w < b->nworkers; ++w) {
            repeated |= b->workers[w].repeated[i] | (seen & b->workers[w].seen[i]);
            seen |= b->workers[w].seen[i];
        }
        b->seen[i] = seen;
        b->repeated[i] = repeated;
    }
}

static void winning_event(Build *b, unsigned worker, const Copy *copy) {
    Slice f[7];
    exact_fields(copy, f);
    emit_event(b, worker, f);
}

static Slice ingestion(const Copy *copy) {
    if (!(copy->fields & LONG_ROW)) return saved_field(copy, 3);
    Slice fields[7];
    exact_fields(copy, fields);
    return fields[3];
}

static bool earlier(const Copy *a, const Copy *b) {
    Slice x = ingestion(a), y = ingestion(b);
    if (x.n != 20 || y.n != 20) fail("invalid ingestion row in duplicate selection");
    return memcmp(x.p, y.p, 20) < 0;
}

static size_t counter_slot(Key id, size_t slots) { return (mix(id) >> 8) & (slots - 1); }

static void note_copy(unsigned char *counts, size_t slot) {
    unsigned shift = (unsigned)(slot % 4) * 2;
    if (((counts[slot / 4] >> shift) & 3) < 2) counts[slot / 4] += (unsigned char)(1u << shift);
}

static bool alone(const unsigned char *counts, size_t slot) {
    return ((counts[slot / 4] >> ((slot % 4) * 2)) & 3) == 1;
}

static void emit_projection(Build *b, unsigned worker, const Projected *p) {
    if (!p->user) return;
    Key person = lookup(b->aliases, p->user, p->user);
    PUSH(&b->workers[worker].packed_people[mix(person) & (PERSON_SHARDS - 1)], ((PackedRouted){person, p->event}));
}

static void dedup_projected(void *data, size_t shard, unsigned worker) {
    Build *b = data;
    size_t rows = 0;
    for (unsigned w = 0; w < b->nworkers; ++w) rows += b->workers[w].projected[shard].n;
    if (rows > (SIZE_MAX - 4) / 8) fail("too many projected events");
    size_t slots = round_power(rows * 8 + 4);
    unsigned char *counts = zero_allocate(slots / 4, 1);
    for (unsigned w = 0; w < b->nworkers; ++w) {
        Projections p = b->workers[w].projected[shard];
        for (size_t i = 0; i < p.n; ++i) note_copy(counts, counter_slot(p.v[i].id, slots));
    }
    Map owners = {0};
    for (unsigned w = 0; w < b->nworkers; ++w) {
        Projections p = b->workers[w].projected[shard];
        for (size_t i = 0; i < p.n; ++i) {
            Projected *copy = &p.v[i];
            if (alone(counts, counter_slot(copy->id, slots))) emit_projection(b, worker, copy);
            else {
                uint64_t *winner = entry(&owners, copy->id, (uintptr_t)copy);
                if (copy->ingest < ((const Projected *)(uintptr_t)*winner)->ingest) *winner = (uintptr_t)copy;
            }
        }
    }
    for (size_t i = 0; i < owners.capacity; ++i) {
        if (owners.keys[i]) emit_projection(b, worker, (const Projected *)(uintptr_t)owners.values[i]);
    }
    for (unsigned w = 0; w < b->nworkers; ++w) free(b->workers[w].projected[shard].v);
    destroy_map(&owners);
    free(counts);
}

static void dedup_shard(void *data, size_t shard, unsigned worker) {
    Build *b = data;
    size_t slots = 0;
    unsigned char *counts = NULL;
    if (options.dedup == DEDUP_FILTER || options.dedup == DEDUP_EAGER) {
        size_t rows = 0;
        for (unsigned w = 0; w < b->nworkers; ++w) rows += b->workers[w].copies[shard].n;
        if (rows > (SIZE_MAX - 4) / 8) fail("too many candidate events");
        slots = round_power(rows * 8 + 4);
        counts = zero_allocate(slots / 4, 1);
        for (unsigned w = 0; w < b->nworkers; ++w) {
            Copies v = b->workers[w].copies[shard];
            for (size_t i = 0; i < v.n; ++i) note_copy(counts, counter_slot(v.v[i].id, slots));
        }
    }
    Map owners = {0};
    for (unsigned w = 0; w < b->nworkers; ++w) {
        Copies v = b->workers[w].copies[shard];
        for (size_t i = 0; i < v.n; ++i) {
            const Copy *copy = &v.v[i];
            if (counts && alone(counts, counter_slot(copy->id, slots))) {
                winning_event(b, worker, copy);
                continue;
            }
            uint64_t *value = entry(&owners, copy->id, (uintptr_t)copy);
            const Copy *previous = (const Copy *)(uintptr_t)*value;
            if (previous != copy && earlier(copy, previous)) *value = (uintptr_t)copy;
        }
    }
    for (size_t i = 0; i < owners.capacity; ++i) {
        if (owners.keys[i]) winning_event(b, worker, (const Copy *)(uintptr_t)owners.values[i]);
    }
    for (unsigned w = 0; w < b->nworkers; ++w) free(b->workers[w].copies[shard].v);
    destroy_map(&owners);
    free(counts);
}

typedef struct { Key key; size_t count; } Numbered;
VECTOR(Numbered, People);
typedef struct { int32_t day; uint32_t category; int64_t cents; } Purchase;
VECTOR(Purchase, Purchases);
VECTOR(size_t, Indices);
typedef struct { int64_t first, last, revenue; uint32_t count; int32_t day; } Bucket;

static int order_events(const void *a, const void *b) {
    int64_t x = ((const Event *)a)->ts, y = ((const Event *)b)->ts;
    return (x > y) - (x < y);
}

static int order_indices(const void *a, const void *b) {
    size_t x = *(const size_t *)a, y = *(const size_t *)b;
    return (x > y) - (x < y);
}

static int order_purchases(const void *a, const void *b) {
    const Purchase *x = a, *y = b;
    if (x->day != y->day) return (x->day > y->day) - (x->day < y->day);
    return (x->category > y->category) - (x->category < y->category);
}

static void sort_people(Event *events, size_t n, Sessions *sessions, Purchases *p) {
    if (n <= 24) {
        for (size_t i = 1; i < n; ++i) {
            Event value = events[i];
            size_t j = i;
            while (j && events[j - 1].ts > value.ts) { events[j] = events[j - 1]; --j; }
            events[j] = value;
        }
    } else qsort(events, n, sizeof *events, order_events);
    Session session = {.start = events[0].ts};
    int64_t previous = session.start;
    int32_t day = utc_day(session.start);
    for (size_t i = 0; i < n; ++i) {
        Event e = events[i];
        if (e.ts - previous > GAP_MS) {
            PUSH(sessions, session);
            session = (Session){.start = e.ts};
            day = utc_day(e.ts);
        }
        ++session.events;
        session.revenue += e.cents;
        if (e.cents) PUSH(p, ((Purchase){day, e.category, e.cents}));
        previous = e.ts;
    }
    PUSH(sessions, session);
}

typedef struct {
    Bucket *buckets;
    int64_t *sums, *daily;
    unsigned char *day_has_purchases;
    uint64_t *bits;
    Indices occupied, touched;
    size_t count, categories, days;
    int32_t base_day;
} Buckets;

static bool bucket_people(Buckets *work, Event *events, size_t n, Sessions *sessions, Purchases *p, int64_t window_start) {
    bool dense = work->sums != NULL;
    for (size_t i = 0; i < n; ++i) {
        Event e = events[i];
        size_t slot = (size_t)((e.ts - window_start) / GAP_MS);
        Bucket *b = &work->buckets[slot];
        if (!b->count) {
            if (work->bits) work->bits[slot / 64] |= UINT64_C(1) << (slot % 64);
            else PUSH(&work->occupied, slot);
            b->first = b->last = e.ts;
        } else {
            if (e.ts < b->first) b->first = e.ts;
            if (e.ts > b->last) b->last = e.ts;
        }
        ++b->count;
        b->revenue += e.cents;
        if (e.cents) {
            if (dense) {
                size_t key = slot * work->categories + e.category;
                if (!work->sums[key]) PUSH(&work->touched, key);
                work->sums[key] += e.cents;
            } else PUSH(p, ((Purchase){(int32_t)slot, e.category, e.cents}));
        }
    }
    if (work->bits) {
        for (size_t word = 0; word < (work->count + 63) / 64; ++word) {
            uint64_t mask = work->bits[word];
            while (mask) {
                PUSH(&work->occupied, 64 * word + (size_t)__builtin_ctzll(mask));
                mask &= mask - 1;
            }
            work->bits[word] = 0;
        }
    } else qsort(work->occupied.v, work->occupied.n, sizeof *work->occupied.v, order_indices);
    int64_t first = work->buckets[work->occupied.v[0]].first, previous = first;
    Session session = {.start = first};
    int32_t day = utc_day(first);
    for (size_t i = 0; i < work->occupied.n; ++i) {
        Bucket *b = &work->buckets[work->occupied.v[i]];
        if (b->first - previous > GAP_MS) {
            PUSH(sessions, session);
            session = (Session){.start = b->first};
            day = utc_day(b->first);
        }
        session.events += b->count;
        session.revenue += b->revenue;
        b->day = day;
        previous = b->last;
    }
    PUSH(sessions, session);
    if (dense) {
        for (size_t i = 0; i < work->touched.n; ++i) {
            size_t key = work->touched.v[i];
            size_t day_index = (size_t)(work->buckets[key / work->categories].day - work->base_day);
            work->daily[day_index * work->categories + key % work->categories] += work->sums[key];
            work->day_has_purchases[day_index] = 1;
            work->sums[key] = 0;
        }
        work->touched.n = 0;
    } else {
        for (size_t i = 0; i < p->n; ++i) p->v[i].day = work->buckets[p->v[i].day].day;
    }
    for (size_t i = 0; i < work->occupied.n; ++i) work->buckets[work->occupied.v[i]] = (Bucket){0};
    work->occupied.n = 0;
    return dense;
}

static void daily_rows(Key key, const Session *s, size_t count, Purchases *p, Buckets *b, bool dense, UserDays *out) {
    if (p->n > 1) qsort(p->v, p->n, sizeof *p->v, order_purchases);
    size_t j = 0;
    for (size_t i = 0; i < count;) {
        int32_t day = utc_day(s[i].start);
        UserDay row = {.person = key, .day = day, .category = NO_CATEGORY};
        do {
            ++row.sessions;
            row.events += s[i].events;
            row.revenue += s[i].revenue;
        } while (++i < count && utc_day(s[i].start) == day);
        int64_t best = 0;
        if (dense) {
            size_t d = (size_t)(day - b->base_day);
            if (b->day_has_purchases[d]) {
                int64_t *totals = &b->daily[d * b->categories];
                for (size_t k = 0; k < b->categories; ++k) {
                    if (totals[k] > best) { best = totals[k]; row.category = (uint32_t)k; }
                    totals[k] = 0;
                }
                b->day_has_purchases[d] = 0;
            }
        } else {
            while (j < p->n && p->v[j].day == day) {
                uint32_t category = p->v[j].category;
                int64_t sum = 0;
                do sum += p->v[j++].cents;
                while (j < p->n && p->v[j].day == day && p->v[j].category == category);
                if (sum > best) { best = sum; row.category = category; }
            }
        }
        PUSH(out, row);
    }
    p->n = 0;
}

static size_t person_number(Timeline *t, People *names, Key key) {
    size_t id = *entry(&t->index, key, names->n);
    if (id == names->n) PUSH(names, ((Numbered){key, 0}));
    ++names->v[id].count;
    return id;
}

static void group_people(void *data, size_t shard, unsigned worker) {
    (void)worker;
    Build *b = data;
    Timeline *t = &b->timelines[shard];
    People names = {0};
    for (unsigned w = 0; w < b->nworkers; ++w) {
        if (b->packed) {
            PackedEvents v = b->workers[w].packed_people[shard];
            for (size_t i = 0; i < v.n; ++i) v.v[i].person = person_number(t, &names, v.v[i].person);
        } else {
            RoutedEvents v = b->workers[w].people[shard];
            for (size_t i = 0; i < v.n; ++i) v.v[i].person = person_number(t, &names, v.v[i].person);
        }
    }
    size_t *starts = allocate(names.n + 1, sizeof *starts), *next = allocate(names.n, sizeof *next);
    size_t total = 0;
    for (size_t i = 0; i < names.n; ++i) {
        starts[i] = next[i] = total;
        total += names.v[i].count;
    }
    starts[names.n] = total;
    Event *grouped = b->packed ? NULL : allocate(total, sizeof *grouped);
    uint64_t *packed = b->packed ? allocate(total, sizeof *packed) : NULL;
    for (unsigned w = 0; w < b->nworkers; ++w) {
        if (packed) {
            PackedEvents v = b->workers[w].packed_people[shard];
            for (size_t i = 0; i < v.n; ++i) packed[next[v.v[i].person]++] = v.v[i].event;
            free(v.v);
        } else {
            RoutedEvents v = b->workers[w].people[shard];
            for (size_t i = 0; i < v.n; ++i) grouped[next[v.v[i].person]++] = v.v[i].event;
            free(v.v);
        }
    }
    free(next);
    t->count = names.n;
    t->people = allocate(names.n, sizeof *t->people);
    const Window *window = b->window;
    Buckets buckets = {
        .count = (size_t)((window->end - window->start - 1) / GAP_MS + 1),
        .categories = b->catalog->category_count,
        .base_day = utc_day(window->start),
        .days = (size_t)(utc_day(window->end - 1) - utc_day(window->start) + 1),
    };
    Purchases purchases = {0};
    Event *unpacked = NULL;
    size_t unpacked_capacity = 0;
    for (size_t i = 0; i < names.n; ++i) {
        size_t n = names.v[i].count, begin = t->sessions.n;
        Event *events;
        if (packed) {
            while (unpacked_capacity < n) unpacked = grow(unpacked, &unpacked_capacity, sizeof *unpacked);
            const uint64_t *source = packed + starts[i];
            for (size_t j = 0; j < n; ++j) {
                uint64_t value = source[j];
                unpacked[j] = (Event){window->start + (int64_t)(value >> 32), (uint32_t)(value & 0xffffff), (uint32_t)((value >> 24) & 255)};
            }
            events = unpacked;
        } else events = grouped + starts[i];
        bool dense = false;
        if (n >= options.bucket_min && buckets.count <= 8192) {
            if (!buckets.buckets) {
                buckets.buckets = zero_allocate(buckets.count, sizeof *buckets.buckets);
                if (options.bucket_bits) buckets.bits = zero_allocate((buckets.count + 63) / 64, sizeof *buckets.bits);
                if (buckets.categories <= 65536 / buckets.count) {
                    buckets.sums = zero_allocate(buckets.count * buckets.categories, sizeof *buckets.sums);
                    buckets.daily = zero_allocate(buckets.days * buckets.categories, sizeof *buckets.daily);
                    buckets.day_has_purchases = zero_allocate(buckets.days, 1);
                }
            }
            dense = bucket_people(&buckets, events, n, &t->sessions, &purchases, window->start);
        } else sort_people(events, n, &t->sessions, &purchases);
        t->people[i] = (Person){begin, t->sessions.n};
        daily_rows(names.v[i].key, t->sessions.v + begin, t->sessions.n - begin, &purchases, &buckets, dense, &t->days);
    }
    free(grouped);
    free(packed);
    free(unpacked);
    free(starts);
    free(names.v);
    free(purchases.v);
    free(buckets.buckets);
    free(buckets.sums);
    free(buckets.daily);
    free(buckets.day_has_purchases);
    free(buckets.bits);
    free(buckets.occupied.v);
    free(buckets.touched.v);
}

void build_sessions(const char *data, Pool *pool, const Window *window, Catalog *catalog, Map *aliases, Timeline *people) {
    char *event_path = path_join(data, "events"), *link_path = path_join(data, "links");
    Build b = {
        .events = open_input(event_path, NULL), .links = open_input(link_path, NULL),
        .products = open_input(data, "products.csv"), .campaigns = open_input(data, "campaigns.csv"),
        .workers = zero_allocate(pool->workers, sizeof(EventWorker)), .nworkers = pool->workers,
        .window = window, .catalog = catalog, .aliases = aliases, .timelines = people,
    };
    free(event_path);
    free(link_path);
    load_input(&b.events, pool);
    load_input(&b.links, pool);
    load_input(&b.products, pool);
    load_input(&b.campaigns, pool);
    if (options.dedup == DEDUP_TWO_SCAN) {
        b.slots = round_power(b.events.bytes / 16 + 1024);
        for (unsigned w = 0; w < b.nworkers; ++w) {
            b.workers[w].seen = zero_allocate(b.slots / 64, sizeof(uint64_t));
            b.workers[w].repeated = zero_allocate(b.slots / 64, sizeof(uint64_t));
        }
    }
    if (options.dedup == DEDUP_EAGER) parallel(pool, 2, prepare, &b);
    else parallel(pool, b.events.nranges + 3, prepare, &b);
    b.packed = options.packed_events && catalog->category_count <= 256 &&
        (uint64_t)(window->end - window->start - 1) <= UINT32_MAX;
    if (options.dedup == DEDUP_EAGER) {
        b.eager = b.packed;
        parallel(pool, b.events.nranges + 1, prepare_eager, &b);
    }
    close_input(&b.links);
    close_input(&b.products);
    close_input(&b.campaigns);
    phase("catalog, identity and event scan");
    if (b.slots) {
        b.seen = allocate(b.slots / 64, sizeof(uint64_t));
        b.repeated = allocate(b.slots / 64, sizeof(uint64_t));
        parallel(pool, (b.slots / 64 + 4095) / 4096, merge_counts, &b);
        for (unsigned w = 0; w < b.nworkers; ++w) {
            free(b.workers[w].seen);
            free(b.workers[w].repeated);
        }
        parallel(pool, b.events.nranges, scan_copies, &b);
        free(b.seen);
        free(b.repeated);
    }
    parallel(pool, EVENT_SHARDS, b.eager ? dedup_projected : dedup_shard, &b);
    close_input(&b.events);
    phase("exact winner selection and routing");
    parallel(pool, PERSON_SHARDS, group_people, &b);
    free(b.workers);
    phase("session and user-day totals");
}
