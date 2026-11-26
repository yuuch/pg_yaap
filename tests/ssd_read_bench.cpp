// Standalone read-only file I/O benchmark. No PostgreSQL runtime dependency.
// clang++ -O3 -std=c++17 -pthread tests/ssd_read_bench.cpp -o /tmp/ssd_read_bench
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <regex>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/uio.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace {
using U64 = uint64_t;
constexpr U64 kBlock = 8192;
constexpr U64 kGiB = U64{1} << 30;

U64 clock_ns(clockid_t clock = CLOCK_MONOTONIC) {
    timespec t{};
    if (clock_gettime(clock, &t) != 0)
        throw std::runtime_error("clock_gettime: " + std::string(std::strerror(errno)));
    return U64(t.tv_sec) * 1000000000 + U64(t.tv_nsec);
}
U64 random_next(U64 &state) {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * UINT64_C(2685821657736338717);
}
std::string quoted(const std::string &s) {
    std::string out = "\"";
    for (unsigned char c : s) {
        if (c == '"' || c == '\\') { out += '\\'; out += char(c); }
        else if (c < 32) {
            const char *hex = "0123456789abcdef";
            out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15];
        } else out += char(c);
    }
    return out + '"';
}
struct Options {
    std::string directory, stem;
    U64 limit_bytes = 64 * kGiB;
    unsigned threads = 1, seconds = 5, warmup = 1, pages = 1;
    std::string pattern = "seq", layout = "contiguous", cache = "nocache", readahead = "off";
    bool self_test = false;
};
U64 number(const std::string &s, const std::string &name) {
    if (s.empty() || s.find_first_not_of("0123456789") != std::string::npos)
        throw std::invalid_argument(name + " requires a nonnegative integer");
    size_t end = 0;
    U64 n = std::stoull(s, &end);
    if (end != s.size()) throw std::invalid_argument("invalid " + name);
    return n;
}
void usage() {
    std::cout << "Usage: ssd_read_bench --directory PATH --stem NUMERIC [--limit-gib 64]\n"
                 "  [--threads 1..128] [--seconds 1..60] [--warmup 0..10]\n"
                 "  [--pattern seq|random] [--pages-per-call 1..128]\n"
                 "  [--layout contiguous|scatter|staged] [--cache nocache|buffered]\n"
                 "  [--readahead off|on] [--self-test]\n"
                 "Files are opened O_RDONLY. Block size is always 8192 bytes.\n";
}
Options options(int argc, char **argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help") { usage(); std::exit(0); }
        if (a == "--self-test") { o.self_test = true; continue; }
        if (++i >= argc) throw std::invalid_argument("missing value for " + a);
        std::string v = argv[i];
        auto bounded = [&](U64 low, U64 high) {
            U64 n = number(v, a);
            if (n < low || n > high) throw std::invalid_argument(a + " outside allowed range");
            return unsigned(n);
        };
        if (a == "--directory") o.directory = v;
        else if (a == "--stem") o.stem = v;
        else if (a == "--limit-gib") o.limit_bytes = U64(bounded(1, 1048576)) * kGiB;
        else if (a == "--threads") o.threads = bounded(1, 128);
        else if (a == "--seconds") o.seconds = bounded(1, 60);
        else if (a == "--warmup") o.warmup = bounded(0, 10);
        else if (a == "--pages-per-call") o.pages = bounded(1, 128);
        else if (a == "--pattern") o.pattern = v;
        else if (a == "--layout") o.layout = v;
        else if (a == "--cache") o.cache = v;
        else if (a == "--readahead") o.readahead = v;
        else throw std::invalid_argument("unknown option: " + a);
    }
    if (o.self_test) return o;
    if (o.directory.empty() || !std::regex_match(o.stem, std::regex("[0-9]+")))
        throw std::invalid_argument("--directory and numeric --stem are required");
    if (o.pattern != "seq" && o.pattern != "random") throw std::invalid_argument("invalid pattern");
    if (o.layout != "contiguous" && o.layout != "scatter" && o.layout != "staged")
        throw std::invalid_argument("invalid layout");
    if (o.cache != "nocache" && o.cache != "buffered") throw std::invalid_argument("invalid cache mode");
    if (o.readahead != "off" && o.readahead != "on") throw std::invalid_argument("invalid readahead mode");
    if (o.layout == "scatter") {
        long max_iov = sysconf(_SC_IOV_MAX);
        if (max_iov < 0) max_iov = 16;
        if (o.pages > U64(max_iov)) throw std::invalid_argument("pages exceed platform IOV_MAX");
    }
    return o;
}
struct File {
    std::string path;
    int fd = -1;
    U64 first_request = 0, requests = 0, logical_bytes = 0, allocated_bytes = 0;
    File() = default;
    File(const File &) = delete;
    File &operator=(const File &) = delete;
    File(File &&other) noexcept : path(std::move(other.path)), fd(other.fd),
        first_request(other.first_request), requests(other.requests),
        logical_bytes(other.logical_bytes), allocated_bytes(other.allocated_bytes) { other.fd = -1; }
    ~File() { if (fd >= 0) close(fd); }
};
struct Dataset {
    std::vector<File> files;
    U64 requests = 0, bytes = 0, allocated_bytes = 0, file_logical_bytes = 0, ignored_tail_bytes = 0;
    explicit Dataset(const Options &o) {
        std::vector<std::pair<U64, std::filesystem::path>> paths;
        std::regex pattern(o.stem + "(?:\\.([0-9]+))?");
        for (const auto &entry : std::filesystem::directory_iterator(o.directory)) {
            std::smatch match;
            std::string name = entry.path().filename().string();
            if (std::regex_match(name, match, pattern) && entry.is_regular_file())
                paths.emplace_back(match[1].matched ? number(match[1].str(), "segment") : 0, entry.path());
        }
        std::sort(paths.begin(), paths.end());
        if (paths.empty()) throw std::invalid_argument("no matching heap segments");
        const U64 request_bytes = U64(o.pages) * kBlock;
        U64 segment = 0, remaining_bytes = o.limit_bytes;
        for (const auto &item : paths) {
            if (!remaining_bytes) break;
            if (item.first != segment++) throw std::invalid_argument("missing or duplicated numeric heap segment");
            File f;
            f.path = item.second.string();
            int flags = O_RDONLY;
#ifndef __APPLE__
#ifdef O_DIRECT
            if (o.cache == "nocache") flags |= O_DIRECT;
#else
            if (o.cache == "nocache") throw std::runtime_error("nocache unsupported on this platform");
#endif
#endif
            f.fd = open(f.path.c_str(), flags);
            if (f.fd < 0) throw std::runtime_error("open " + f.path + ": " + std::strerror(errno));
            struct stat st{};
            if (fstat(f.fd, &st) != 0) throw std::runtime_error("fstat: " + std::string(std::strerror(errno)));
            if (!S_ISREG(st.st_mode) || st.st_size <= 0 || U64(st.st_size) % kBlock)
                throw std::invalid_argument("segment must be nonempty and 8KiB aligned: " + f.path);
#ifdef __APPLE__
            if (fcntl(f.fd, F_NOCACHE, o.cache == "nocache" ? 1 : 0) < 0)
                throw std::runtime_error("F_NOCACHE: " + std::string(std::strerror(errno)));
            if (fcntl(f.fd, F_RDAHEAD, o.readahead == "on" ? 1 : 0) < 0)
                throw std::runtime_error("F_RDAHEAD: " + std::string(std::strerror(errno)));
#else
            int advice = o.readahead == "on" ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_RANDOM;
            int ret = posix_fadvise(f.fd, 0, 0, advice);
            if (ret) throw std::runtime_error("posix_fadvise: " + std::string(std::strerror(ret)));
#endif
            f.logical_bytes = U64(st.st_size);
            f.allocated_bytes = U64(st.st_blocks) * 512;
            U64 selected = std::min(f.logical_bytes, remaining_bytes);
            remaining_bytes -= selected;
            f.requests = selected / request_bytes;
            ignored_tail_bytes += selected % request_bytes;
            if (!f.requests) { if (selected < f.logical_bytes) break; else continue; }
            f.first_request = requests;
            requests += f.requests;
            bytes += f.requests * request_bytes;
            allocated_bytes += f.allocated_bytes;
            file_logical_bytes += f.logical_bytes;
            files.push_back(std::move(f));
            if (selected < U64(st.st_size)) break;
        }
        if (requests < o.threads) throw std::invalid_argument("dataset has fewer complete requests than threads");
    }
    const File &locate(U64 request) const {
        auto it = std::upper_bound(files.begin(), files.end(), request,
            [](U64 n, const File &f) { return n < f.first_request; });
        if (it == files.begin()) throw std::runtime_error("request mapping underflow");
        --it;
        if (request - it->first_request >= it->requests) throw std::runtime_error("request mapping overflow");
        return *it;
    }
};
struct Aligned {
    void *p = nullptr;
    explicit Aligned(size_t bytes) {
        if (!bytes) return;
        int ret = posix_memalign(&p, kBlock, bytes);
        if (ret) throw std::runtime_error("posix_memalign: " + std::string(std::strerror(ret)));
        std::memset(p, 0, bytes); // Fault in memory before timing starts.
    }
    ~Aligned() { std::free(p); }
    Aligned(const Aligned &) = delete;
};
struct Stats {
    U64 operations = 0, sampled = 0, finish_ns = 0, cpu_ns = 0, max_latency_ns = 0;
    U64 checksum = 0;
    std::array<U64, 64> latency{};
    std::string error;
};
unsigned bucket(U64 ns) { return ns ? std::min(63u, 64u - unsigned(__builtin_clzll(ns))) : 0; }
U64 quantile(const std::array<U64, 64> &bins, U64 count, unsigned percent, U64 maximum) {
    if (!count) return 0;
    U64 target = (count * percent + 99) / 100, seen = 0;
    for (unsigned i = 0; i < bins.size(); ++i) {
        seen += bins[i];
        if (seen >= target) return std::min(maximum, i == 63 ? UINT64_MAX : (U64{1} << i) - 1);
    }
    throw std::runtime_error("invalid histogram");
}
void self_test() {
    std::array<U64, 64> bins{};
    for (U64 n : {U64(0), U64(1), U64(2), U64(3), U64(9)}) ++bins[bucket(n)];
    if (quantile(bins, 5, 50, 9) != 3 || quantile(bins, 5, 99, 9) != 9)
        throw std::runtime_error("histogram self-test failed");
    for (U64 n : {U64(1), U64(2), U64(17), U64(1000)}) {
        for (U64 threads = 1; threads <= std::min<U64>(128, n); ++threads) {
            U64 end = 0;
            for (U64 i = 0; i < threads; ++i) {
                U64 begin = n * i / threads, next = n * (i + 1) / threads;
                if (begin != end || next <= begin) throw std::runtime_error("partition self-test failed");
                end = next;
            }
            if (end != n) throw std::runtime_error("partition coverage failed");
        }
    }
    std::cout << "{\"self_test\":\"PASS\",\"checks\":\"histogram_and_disjoint_partition\"}\n";
}
void run(const Options &o, const Dataset &data) {
    std::mutex mutex;
    std::condition_variable cv;
    unsigned ready = 0;
    bool go = false;
    U64 measure_start = 0, deadline = 0;
    std::atomic<bool> stop{false};
    std::vector<Stats> stats(o.threads);
    std::vector<std::thread> threads;
    const size_t bytes = size_t(o.pages) * kBlock;
    try {
    for (unsigned index = 0; index < o.threads; ++index) {
        threads.emplace_back([&, index] {
            Stats &s = stats[index];
            bool signalled_ready = false;
            try {
                Aligned read_buffer(o.layout == "scatter" ? 0 : bytes);
                Aligned scatter_buffer(o.layout == "contiguous" ? 0 : bytes * 2);
                std::vector<iovec> iov(o.layout == "contiguous" ? 0 : o.pages);
                for (unsigned i = 0; i < iov.size(); ++i)
                    iov[i] = {static_cast<char *>(scatter_buffer.p) + size_t(i) * kBlock * 2, size_t(kBlock)};
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    ++ready; signalled_ready = true; cv.notify_all();
                    cv.wait(lock, [&] { return go; });
                }
                const U64 first = data.requests * index / o.threads;
                const U64 end = data.requests * (index + 1) / o.threads;
                U64 pos = first, rng = UINT64_C(0x9e3779b97f4a7c15) ^ (index + 1);
                U64 sample_rng = UINT64_C(0xd1b54a32d192ed03) ^ (index + 1);
                U64 cached_now = clock_ns(), cpu_start = 0, iterations = 0;
                bool measuring = false;
                while (!stop.load(std::memory_order_relaxed)) {
                    if ((iterations++ & 31) == 0) cached_now = clock_ns();
                    if (cached_now >= deadline) break;
                    if (!measuring && cached_now >= measure_start) {
                        cpu_start = clock_ns(CLOCK_THREAD_CPUTIME_ID);
                        measuring = true;
                    }
                    U64 request = o.pattern == "random" ? first + random_next(rng) % (end - first) : pos;
                    const File &f = data.locate(request);
                    off_t offset = off_t((request - f.first_request) * bytes);
                    bool sampled = measuring && (random_next(sample_rng) >> 58) == 0;
                    U64 begin = sampled ? clock_ns() : 0;
                    ssize_t got = o.layout == "scatter"
                        ? preadv(f.fd, iov.data(), int(iov.size()), offset)
                        : pread(f.fd, read_buffer.p, bytes, offset);
                    const int saved_errno = errno;
                    U64 elapsed = sampled ? clock_ns() - begin : 0;
                    if (got < 0) throw std::runtime_error("pread/preadv: " + std::string(std::strerror(saved_errno)));
                    if (U64(got) != bytes) throw std::runtime_error("short read: requested " + std::to_string(bytes) + ", got " + std::to_string(got));
                    if (o.layout == "staged") {
                        for (unsigned i = 0; i < o.pages; ++i)
                            std::memcpy(iov[i].iov_base, static_cast<char *>(read_buffer.p) + size_t(i) * kBlock, size_t(kBlock));
#if defined(__GNUC__) || defined(__clang__)
                        asm volatile("" : : "r"(scatter_buffer.p) : "memory");
#endif
                    }
                    if (measuring) {
                        ++s.operations;
                        s.checksum ^= static_cast<unsigned char *>(o.layout == "contiguous" ? read_buffer.p : scatter_buffer.p)[0];
                        if (sampled) {
                            ++s.sampled; ++s.latency[bucket(elapsed)];
                            s.max_latency_ns = std::max(s.max_latency_ns, elapsed);
                        }
                    }
                    if (++pos == end) pos = first;
                }
                s.finish_ns = clock_ns();
                if (measuring) s.cpu_ns = clock_ns(CLOCK_THREAD_CPUTIME_ID) - cpu_start;
            } catch (...) {
                try { throw; }
                catch (const std::exception &e) { s.error = e.what(); }
                catch (...) { s.error = "unknown worker exception"; }
                stop.store(true, std::memory_order_relaxed);
                if (!signalled_ready) {
                    std::lock_guard<std::mutex> lock(mutex);
                    ++ready; cv.notify_all();
                }
            }
        });
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [&] { return ready == o.threads; });
        measure_start = clock_ns() + U64(o.warmup) * 1000000000;
        deadline = measure_start + U64(o.seconds) * 1000000000;
        go = true; cv.notify_all();
    }
    } catch (...) {
        stop.store(true, std::memory_order_relaxed);
        { std::lock_guard<std::mutex> lock(mutex); go = true; cv.notify_all(); }
        for (auto &thread : threads) if (thread.joinable()) thread.join();
        throw;
    }
    for (auto &thread : threads) thread.join();
    U64 operations = 0, samples = 0, cpu = 0, finish = 0, maximum = 0, checksum = 0;
    std::array<U64, 64> bins{};
    for (const auto &s : stats) {
        if (!s.error.empty()) throw std::runtime_error(s.error);
        operations += s.operations; samples += s.sampled; cpu += s.cpu_ns;
        finish = std::max(finish, s.finish_ns); maximum = std::max(maximum, s.max_latency_ns);
        checksum ^= s.checksum;
        for (unsigned i = 0; i < bins.size(); ++i) bins[i] += s.latency[i];
    }
    if (!operations || finish <= measure_start) throw std::runtime_error("no measured operations");
    double seconds = double(finish - measure_start) / 1e9;
    U64 read_bytes = operations * bytes;
    std::cout << std::fixed << std::setprecision(6)
        << "{\"block_bytes\":" << kBlock << ",\"pages_per_call\":" << o.pages
        << ",\"request_bytes\":" << bytes << ",\"threads\":" << o.threads
        << ",\"pattern\":" << quoted(o.pattern) << ",\"layout\":" << quoted(o.layout)
        << ",\"cache\":" << quoted(o.cache) << ",\"readahead\":" << quoted(o.readahead)
        << ",\"dataset_bytes\":" << data.bytes << ",\"dataset_allocated_bytes\":" << data.allocated_bytes
        << ",\"selected_file_logical_bytes\":" << data.file_logical_bytes
        << ",\"allocation_semantic\":\"WHOLE_SELECTED_FILES_NOT_PREFIX_EXTENT_PROOF\""
        << ",\"ignored_alignment_tail_bytes\":" << data.ignored_tail_bytes
        << ",\"files\":" << data.files.size() << ",\"read_bytes\":" << read_bytes
        << ",\"operations\":" << operations << ",\"elapsed_seconds\":" << seconds
        << ",\"requested_seconds\":" << o.seconds << ",\"warmup_seconds\":" << o.warmup
        << ",\"MiB_s\":" << read_bytes / seconds / (1024.0 * 1024)
        << ",\"MB_s\":" << read_bytes / seconds / 1000000.0
        << ",\"iops\":" << operations / seconds << ",\"page_iops\":" << operations * double(o.pages) / seconds
        << ",\"cpu_seconds\":" << cpu / 1e9 << ",\"cpu_cores\":" << cpu / 1e9 / seconds
        << ",\"latency_sample_count\":" << samples
        << ",\"latency_sample_rate\":64,\"latency_semantic\":\"SYSCALL_SAMPLE_BUCKET_UPPER_BOUND_NOT_COPY_OR_DEVICE_ONLY\""
        << ",\"latency_available\":" << (samples ? "true" : "false")
        << ",\"p50_ns_le\":" << (samples ? std::to_string(quantile(bins, samples, 50, maximum)) : "null")
        << ",\"p95_ns_le\":" << (samples ? std::to_string(quantile(bins, samples, 95, maximum)) : "null")
        << ",\"p99_ns_le\":" << (samples ? std::to_string(quantile(bins, samples, 99, maximum)) : "null")
        << ",\"max_sampled_latency_ns\":" << maximum << ",\"checksum\":" << checksum
        << ",\"staged_copy_included\":" << (o.layout == "staged" ? "true" : "false")
        << ",\"errors\":0}\n";
}
} // namespace
int main(int argc, char **argv) {
    try {
        Options o = options(argc, argv);
        if (o.self_test) { self_test(); return 0; }
        Dataset dataset(o);
        run(o, dataset);
        return 0;
    } catch (const std::invalid_argument &e) {
        std::cerr << "ssd_read_bench configuration: " << e.what() << '\n';
        return 2;
    } catch (const std::exception &e) {
        std::cerr << "ssd_read_bench: " << e.what() << '\n';
        return 1;
    }
}
