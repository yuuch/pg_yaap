# Read-only 8KiB SSD-path demo

This standalone C++17 program measures file-read throughput and IOPS without
PostgreSQL,Buffer Manager or SQL execution. It opens existing numeric heap
segments O_RDONLY and never writes them. The benchmark operates on raw bytes;
it is not an alternative MVCC-safe query scan.

```sh
clang++ -O3 -std=c++17 -pthread tests/ssd_read_bench.cpp -o /tmp/ssd_read_bench
/tmp/ssd_read_bench --self-test
/tmp/ssd_read_bench \
  --directory /Users/chenyunwen/proj/postgres/bench-instance/data/base/106520 \
  --stem 106585 --limit-gib 64 \
  --threads 12 --seconds 5 --warmup 1 \
  --pattern seq --pages-per-call 1 --layout contiguous \
  --cache nocache --readahead off
```

Use a dedicated static dataset with no competing database/storage activity.
These paths identify this workspace's SF100 lineitem heap. Numeric segments
are sorted and must be consecutive; `_fsm`/`_vm` forks are excluded. The first
64GiB is fully allocated and exceeds the machine's24GiB RAM. No large test
file,raw-device read,system cache purge or administrator permission is required.
Do not change an active database's files;this program only reads them.

## Request size and memory layout

`--pages-per-call 1` means exactly one8192-byte pread per operation. Sequential
threads walk disjoint regions;random threads choose random request units within
their respective regions. A512-byte/4KiB device sector is not assumed to equal
the application request. The filesystem/driver may split/merge requests.

Optional128-page controls still use8KiB memory pages but transfer1MiB/call:

- `contiguous`: one pread into a contiguous destination.
- `scatter`: one preadv into1288KiB destinations separated by8KiB gaps.
- `staged`: contiguous pread followed by page copies into scattered destinations.
  Copy cost is included in throughput/CPU,but not syscall latency samples.

These controls must not be described as8KiB-request measurements. They help
separate small-request IOPS from large-request bandwidth and vector layout.
They are private memory,without PGBuffer pins/visibility/checksum overhead.

## Cache control and precision

On macOS,nocache calls fcntl(F_NOCACHE,1);readahead off callsF_RDAHEAD,0.
Failures are errors,not silent fallback. F_NOCACHE is a file cache-retention
policy,not a guarantee of LinuxO_DIRECT/zerocopy or eviction of all preexisting
cache data. On supported Linux,nocache usesO_DIRECT;that path is not validated
by this macOS run. Readahead-on/buffered controls explicitly permit more caching
and must not define the claimed SSD-only maximum.

Thus results estimate the machine's8KiB/file-read path underAPFS/OS scheduling,
not a certified raw-device physical maximum. Compare todevice telemetry and
repeat;do not equate processbytes with physicalSSDbytes blindly. Datasetlarger
thanRAM andnocache reducecachebias but do not proveallrequests reachedflash.

## Output and limits

One JSON row includes request size,thread count,readbytes,IOPS,decimalMB/s and
binaryMiB/s,actual measured elapsed,thread user+systemCPU sum andCPUcores,
sampled syscall-latency bucket upper bounds and allocationmetadata. File
allocation describes whole selected files;partial-prefix allocation cannot be
proved fromst_blocks alone. Short/errorreads abort rather than count as success.
Memory is pre-touched beforethe start barrier. Warm-up precedesmeasurement;
endchecks are batched32calls andactualelapsed includesdeadlineovershoot.

Latency is sampled~1/64calls into fixed64log buckets. Reportedpercentiles are
upper bounds,not exactquantiles or pureSSD latency. CPUtime excludes other
processes and may not include all interrupt/driver work. No CPU-time ratio
alone identifies physicalIOwait.

The regression script uses tiny cached fixtures for correctness only:

```sh
SSD_READ_BENCH=/tmp/ssd_read_bench python3 tests/test_ssd_read_bench.py
```

The artifactrunner records per-case iostat captures,requests/bytes consistency,
and memorypressure. Its initialsince-bootiostatrow andfirstongoingwarm-uprow
are excluded from steady device summaries. Device stats are whole-hostcombined
IO,and are corroborating evidence rather than query-private physical counters.
