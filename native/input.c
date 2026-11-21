#include "shopping.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <immintrin.h>
#include <liburing.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

VECTOR(InputFile, InputFiles);
VECTOR(Range, Ranges);

static void open_file(InputFiles *files, char *path) {
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NONBLOCK);
    if (fd < 0) fail("cannot open %s: %s", path, strerror(errno));
    struct stat st;
    if (fstat(fd, &st)) fail("cannot stat %s: %s", path, strerror(errno));
    if (!S_ISREG(st.st_mode)) {
        close(fd);
        free(path);
        return;
    }
    if (st.st_size < 0 || (uintmax_t)st.st_size > PTRDIFF_MAX) fail("input too large: %s", path);
    size_t size = (size_t)st.st_size;
    if (options.read_hint != HINT_NONE) {
        int hint = options.read_hint == HINT_SEQUENTIAL ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_WILLNEED;
        (void)posix_fadvise(fd, 0, 0, hint);
    }
    InputFile f = {.path = path, .fd = fd, .direct_fd = -1, .size = size};
    if (size) {
        if (options.read == READ_MAP) {
            void *ptr = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (ptr != MAP_FAILED) {
                f.data = ptr;
                f.mapped = true;
            }
        }
        if (!f.mapped) {
            if (options.read == READ_DIRECT) {
                int rc = posix_memalign((void **)&f.data, 4096, size);
                if (rc) fail("cannot allocate direct-read buffer: %s", strerror(rc));
                f.direct_fd = open(path, O_RDONLY | O_CLOEXEC | O_DIRECT);
            } else {
                f.data = allocate(size, 1);
            }
        }
    }
    PUSH(files, f);
}

static int biggest_first(const void *a, const void *b) {
    size_t x = ((const InputFile *)a)->size, y = ((const InputFile *)b)->size;
    return (y > x) - (y < x);
}

Input open_input(const char *dir, const char *name) {
    InputFiles files = {0};
    if (name) {
        open_file(&files, path_join(dir, name));
        if (!files.n) fail("input is not a regular file: %s", name);
    } else {
        DIR *stream = opendir(dir);
        if (!stream) fail("cannot list %s: %s", dir, strerror(errno));
        for (;;) {
            errno = 0;
            struct dirent *e = readdir(stream);
            if (!e) {
                if (errno) fail("cannot list %s: %s", dir, strerror(errno));
                break;
            }
            size_t n = strlen(e->d_name);
            if (n >= 4 && !memcmp(e->d_name + n - 4, ".csv", 4)) open_file(&files, path_join(dir, e->d_name));
        }
        closedir(stream);
    }
    if (files.n > 1) qsort(files.v, files.n, sizeof *files.v, biggest_first);
    Ranges ranges = {0};
    size_t bytes = 0;
    for (size_t i = 0; i < files.n; ++i) {
        InputFile *f = &files.v[i];
        if (f->size > SIZE_MAX - bytes) fail("input byte count overflow");
        bytes += f->size;
        for (size_t start = 0; start < f->size;) {
            size_t n = f->size - start;
            size_t end = start + (n < options.chunk ? n : options.chunk);
            PUSH(&ranges, ((Range){f, start, end}));
            start = end;
        }
    }
    return (Input){files.v, files.n, ranges.v, ranges.n, bytes};
}

static void read_exact(const InputFile *f, size_t start, size_t end) {
    while (start < end) {
        ssize_t n = pread(f->fd, f->data + start, end - start, (off_t)start);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) fail("read failed on %s: %s", f->path, strerror(errno));
        if (!n) fail("unexpected end of input: %s", f->path);
        start += (size_t)n;
    }
}

static void load_range(void *data, size_t job, unsigned worker) {
    (void)worker;
    Range r = ((Input *)data)->ranges[job];
    InputFile *f = r.file;
    if (f->mapped) return;
    size_t start = r.start, aligned_end = r.end & ~(size_t)4095;
    if (f->direct_fd >= 0 && !(start & 4095) && start < aligned_end) {
        ssize_t n;
        do n = pread(f->direct_fd, f->data + start, aligned_end - start, (off_t)start);
        while (n < 0 && errno == EINTR);
        if (n > 0) start += (size_t)n;
        else if (n < 0 && errno != EINVAL && errno != EOPNOTSUPP && errno != ENOTTY)
            fail("direct read failed on %s: %s", f->path, strerror(errno));
    }
    read_exact(f, start, r.end);
}

static bool load_uring(Input *in) {
    enum { DEPTH = 16 };
    struct io_uring ring;
    if (io_uring_queue_init(DEPTH, &ring, 0) < 0) return false;
    for (size_t base = 0; base < in->nranges; base += DEPTH) {
        size_t count = in->nranges - base;
        if (count > DEPTH) count = DEPTH;
        for (size_t j = 0; j < count; ++j) {
            Range *r = &in->ranges[base + j];
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            if (!sqe) fail("I/O submission queue exhausted");
            io_uring_prep_read(sqe, r->file->fd, r->file->data + r->start, (unsigned)(r->end - r->start), r->start);
            io_uring_sqe_set_data(sqe, r);
        }
        size_t submitted = 0;
        while (submitted < count) {
            int n = io_uring_submit(&ring);
            if (n == -EINTR) continue;
            if (n <= 0) fail("I/O submission failed: %s", strerror(n < 0 ? -n : EIO));
            submitted += (size_t)n;
        }
        for (size_t j = 0; j < count;) {
            struct io_uring_cqe *cqe;
            int rc = io_uring_wait_cqe(&ring, &cqe);
            if (rc == -EINTR) continue;
            if (rc < 0) fail("I/O completion failed: %s", strerror(-rc));
            Range *r = io_uring_cqe_get_data(cqe);
            int n = cqe->res;
            io_uring_cqe_seen(&ring, cqe);
            if (n < 0 && n != -EINTR && n != -EINVAL && n != -EOPNOTSUPP && n != -EPERM)
                fail("read failed on %s: %s", r->file->path, strerror(-n));
            size_t pos = r->start + (n > 0 ? (size_t)n : 0);
            read_exact(r->file, pos, r->end);
            ++j;
        }
    }
    io_uring_queue_exit(&ring);
    return true;
}

void load_input(Input *in, Pool *pool) {
    bool needs_read = false;
    for (size_t i = 0; i < in->count; ++i) needs_read |= in->files[i].size && !in->files[i].mapped;
    if (!needs_read) return;
    if (options.read == READ_URING && load_uring(in)) return;
    parallel(pool, in->nranges, load_range, in);
}

void close_input(Input *in) {
    for (size_t i = 0; i < in->count; ++i) {
        InputFile *f = &in->files[i];
        if (f->mapped) munmap(f->data, f->size);
        else free(f->data);
        if (f->direct_fd >= 0) close(f->direct_fd);
        close(f->fd);
        free(f->path);
    }
    free(in->files);
    free(in->ranges);
    *in = (Input){0};
}

bool parse_fields(Slice row, Slice *fields, unsigned columns) {
    const unsigned char *p = row.p, *end = p + row.n;
    for (unsigned i = 0; i < columns; ++i) {
        const unsigned char *comma = memchr(p, ',', (size_t)(end - p));
        if ((comma == NULL) != (i + 1 == columns)) return false;
        const unsigned char *stop = comma ? comma : end;
        const unsigned char *first = p;
        if (first < stop && *first == '"') {
            if (stop - first < 2 || stop[-1] != '"') return false;
            ++first;
            --stop;
        }
        if (memchr(first, '"', (size_t)(stop - first))) return false;
        fields[i] = (Slice){first, (size_t)(stop - first)};
        if (comma) p = comma + 1;
    }
    return true;
}

typedef struct { uint64_t lf, comma, quote; } Marks;

static Marks marks_avx2(const unsigned char *p) {
    __m256i bytes = _mm256_loadu_si256((const __m256i *)p);
    return (Marks){
        (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8('\n'))),
        (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(','))),
        (uint32_t)_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8('"'))),
    };
}

static Marks marks_asm(const unsigned char *p) {
    __m256i bytes = _mm256_loadu_si256((const __m256i *)p);
    __m256i a, b, c;
    uint32_t lf, comma, quote;
    __asm__("vpcmpeqb %[nl], %[bytes], %[a]\n\t"
            "vpcmpeqb %[sep], %[bytes], %[b]\n\t"
            "vpcmpeqb %[qt], %[bytes], %[c]\n\t"
            "vpmovmskb %[a], %[lf]\n\t"
            "vpmovmskb %[b], %[comma]\n\t"
            "vpmovmskb %[c], %[quote]"
            : [a] "=&x"(a), [b] "=&x"(b), [c] "=&x"(c), [lf] "=r"(lf), [comma] "=r"(comma), [quote] "=r"(quote)
            : [bytes] "x"(bytes), [nl] "x"(_mm256_set1_epi8('\n')), [sep] "x"(_mm256_set1_epi8(',')), [qt] "x"(_mm256_set1_epi8('"')));
    return (Marks){lf, comma, quote};
}

__attribute__((target("avx512f,avx512bw")))
static Marks marks_avx512(const unsigned char *p) {
    __m512i bytes = _mm512_loadu_si512(p);
    return (Marks){
        _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('\n')),
        _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8(',')),
        _mm512_cmpeq_epi8_mask(bytes, _mm512_set1_epi8('"')),
    };
}

typedef struct {
    Slice fields[7];
    const unsigned char *row, *field;
    unsigned count, quotes, columns;
    bool bad;
    RowVisitor visit;
    void *context;
    unsigned worker;
} Parse;

static inline void delimiter(Parse *s, const unsigned char *end, bool newline) {
    if (s->count >= s->columns) s->bad = true;
    if (!s->bad) {
        Slice value = {s->field, (size_t)(end - s->field)};
        if (s->quotes) {
            if (s->quotes == 2 && value.n >= 2 && value.p[0] == '"' && end[-1] == '"') {
                ++value.p;
                value.n -= 2;
            } else {
                s->bad = true;
            }
        }
        s->fields[s->count++] = value;
    }
    s->quotes = 0;
    s->field = end + 1;
    if (newline) {
        if (!s->bad && s->count == s->columns)
            s->visit(s->context, s->worker, (Slice){s->row, (size_t)(end - s->row)}, s->fields);
        s->count = 0;
        s->bad = false;
        s->row = s->field;
    }
}

static inline void consume_marks(const unsigned char *p, Parse *s, Marks m) {
    uint64_t structural = m.lf | m.comma;
    while (structural) {
        unsigned pos = (unsigned)__builtin_ctzll(structural);
        uint64_t bit = UINT64_C(1) << pos;
        if (m.quote) {
            s->quotes += (unsigned)__builtin_popcountll(m.quote & (bit - 1));
            m.quote &= ~((bit - 1) | bit);
        }
        delimiter(s, p + pos, (m.lf & bit) != 0);
        structural &= structural - 1;
    }
    s->quotes += (unsigned)__builtin_popcountll(m.quote);
    if (s->quotes > 2) s->quotes = 3;
}

static inline Marks classify(const unsigned char *p) {
    if (options.scan == SCAN_AVX512) return marks_avx512(p);
    if (options.scan == SCAN_ASM) return marks_asm(p);
    return marks_avx2(p);
}

static void scan_marks(const unsigned char *p, const unsigned char *end, Parse *s) {
    size_t width = options.scan == SCAN_AVX512 ? 64 : 32;
    if (options.scan_unroll > 1) {
        size_t block = width * options.scan_unroll;
        while ((size_t)(end - p) >= block) {
            Marks marks[4];
            for (unsigned i = 0; i < options.scan_unroll; ++i) marks[i] = classify(p + width * i);
            for (unsigned i = 0; i < options.scan_unroll; ++i) consume_marks(p + width * i, s, marks[i]);
            p += block;
        }
    }
    while ((size_t)(end - p) >= width) {
        consume_marks(p, s, classify(p));
        p += width;
    }
    for (; p < end; ++p) {
        unsigned char byte = *p;
        if (byte == ',' || byte == '\n') delimiter(s, p, byte == '\n');
        else if (byte == '"' && s->quotes < 3) ++s->quotes;
    }
    if (s->row < end) {
        if (s->count + 1 == s->columns && !s->bad) {
            Slice row = {s->row, (size_t)(end - s->row)};
            if (parse_fields(row, s->fields, s->columns)) s->visit(s->context, s->worker, row, s->fields);
        }
    }
}

void scan_range(const Range *r, unsigned columns, RowVisitor visit, void *ctx, unsigned worker) {
    const unsigned char *data = r->file->data, *limit = data + r->file->size;
    const unsigned char *start = data + r->start, *end = data + r->end;
    if (!r->start || start[-1] != '\n') {
        const unsigned char *skip = memchr(start, '\n', (size_t)(end - start));
        if (!skip || skip + 1 == end) return;
        start = skip + 1;
    }
    if (end < limit && end[-1] != '\n') {
        const unsigned char *lf = memchr(end, '\n', (size_t)(limit - end));
        end = lf ? lf + 1 : limit;
    }
    if (options.scan != SCAN_SCALAR) {
        Parse s = {.row = start, .field = start, .columns = columns, .visit = visit, .context = ctx, .worker = worker};
        scan_marks(start, end, &s);
        return;
    }
    while (start < end) {
        const unsigned char *lf = memchr(start, '\n', (size_t)(end - start));
        const unsigned char *stop = lf ? lf : end;
        Slice row = {start, (size_t)(stop - start)}, fields[7];
        if (parse_fields(row, fields, columns)) visit(ctx, worker, row, fields);
        start = lf ? lf + 1 : end;
    }
}
