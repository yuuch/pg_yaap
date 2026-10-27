# Scan I/O diagnosis contract and conservative rules

This design review covers the current YAAP queue and Scan Profiler. It does not
change production code or the prefetch policy. PostgreSQL API observability is
reviewed separately in `scan_io_observability.md`.

## Current evidence that must not be relabelled

- `scan_profile.hpp::ScanProfileRecord` contains elapsed source/API counters,
  sampled tuple estimates, sampled thread CPU, PG buffer-accounting deltas and
  queue counters. These have different semantics and denominators.
- `QueueRefill` counts `requests` for every StartReadBuffers API invocation;
  accepted pages include hits, split suffixes and actual reads. A short
  accepted API call is not necessarily a small physical I/O.
- `ready_pages` currently counts pages accepted by Start without a required
  Wait. It is submission-time ready, not consumption-time ready.
- `QueueWait` counts the whole WaitReadBuffers call. Calls can complete without
  sleeping, execute synchronous work, handle errors or retry. Neither the call
  count nor its wall duration is an exact I/O-stall counter.
- `inflight` is the number of accepted need_wait operations not yet Waited.
  Their I/O may already have finished. It is not live disk concurrency or the
  PG global submission queue depth.
- `pinned` reserves the assembled batch's entire count, including unsubmitted
  suffixes. It is a conservative page reservation, not exact PG pin ownership.
- Existing `pin_limit_stops` checks additional-pin allowance zero, not all
  partial clamps. Page-window exhaustion may also be present. It does not
  alone establish a pin bottleneck.
- The granted ring is constructed from a window already reduced by YAAP's
  fair-share/backend budgets. `effective == ring_capacity` therefore does not
  itself prove that an independently small ring limited lookahead.

Relevant code boundaries are QueueRefill's quota/pin decisions, StartReadBuffers
invocation and accepted prefix, QueueWait, YaapReadStreamNext's submitted versus
consumed cursors, and GetLocalSourceState's configured/pool/backend window min.

## Pure rule-module input

The evaluator should receive a value object and return a bounded array of
findings. It must not read PG shared memory, query SQL, sample the OS or change
execution. Recommended fields:

```text
scope: query / pipeline / operator / worker, relation/fork, PID, stream mode
complete: bool, publication_conflict: bool
metrics: value + availability + source + semantic + observation_count
configuration: requested pages, ring pages, pin budget, request quota,
               block size, io method/workers, io combine limit
counters: consumed and readiness-classified pages, initial-cache categories,
          paired late/on-time outcomes, exact limiting-branch outcomes,
          create/reset/end counts, API request/accept counts
histograms: bounded request sizes, page lead, request observed completion,
            sampled effective lookahead (with observation weight)
timing: nonoverlapping measured envelopes + explicit unavailable pieces
external evidence: optional, scope/time-bound and independently validated
```

Availability is EXACT / ESTIMATED / UNAVAILABLE. Also retain a semantic tag:
EXACT_VALUE, API_ENVELOPE, OBSERVED_UPPER_BOUND, SAMPLED, PG_COUNTER_DELTA or
EXTERNAL_OBSERVATION. For example, the first observed completion timestamp can
be an exact observation but only an upper bound on actual completion latency.
Do not discard that distinction by marking the entire latency simply EXACT.

An unavailable value is not zero. Zero samples or zero denominator produce
UNAVAILABLE ratios/quantiles. Partial queries may report observed events, but
cannot claim complete-query rates, times or absence of a bottleneck.

## Counting and histogram invariants

Use one consistent page-classification point. Consumption must not become a
second initial buffer lookup/cache hit. Define:

```text
consumed = consume_ready + consume_not_ready + consume_readiness_unknown
initial_lookups = initial_ready_hit + initial_miss + already_loading + unknown
```

Only emit a full consume-ready percentage when the unknown population is zero;
otherwise show classified-subset rate and/or lower/upper bounds separately.
Cold-ready needs a reliably classified initial miss population. Start's
need_wait bit alone does not identify a miss versus already-loading, so it
cannot define exact cold pages.

Keep request-level latency distributions separate from page-level lead
histograms. A merged I/O request supplies one request-latency observation but
can supply many page-consumption observations. Weighted page quantities must
explicitly retain the page weight. Never average worker percentiles: merge
bucket counts, then calculate the aggregate quantile.

A fixed log2 histogram with 64 bins is sufficient. Emit quantile bucket bounds
or an explicitly approximate upper bound, including overflow semantics.
Histogram sample count must equal its represented observation population.
Paired readiness/late outcomes are stronger evidence than independent P95s.
Two unrelated P95s cannot establish individual late requests or lost time.

## Finding representation and ranking

Each finding should contain code, status, evidence list, interpretation,
next experiment and an optional measured impact in worker nanoseconds.
Statuses are CONFIRMED, SUSPECTED, INSUFFICIENT_DATA. A separate confidence
label can describe evidence quality but must not contradict the status.

CONFIRMED applies to directly observed facts (e.g. a specific limiting branch
was taken), not automatically to a claim that removing it improves query time.
Rank by reliably attributed loss when available; never turn event counts into
an invented duration. Unknown impact sorts after measured-impact findings,
with a deterministic rule order. Multiple causes may coexist.

## Rule recommendations

| Code | Safe firing conditions | Status and interpretation |
|---|---|---|
| LOOKAHEAD_PIN_LIMITED | Exact binding-pin reason at setup or refill, with a desired-window deficit; sustained events and linked not-ready demand improve impact evidence | CONFIRMED limit fact; performance consequence SUSPECTED unless linked loss is measured |
| LOOKAHEAD_IO_LIMITED | Own request quota actually stops otherwise-eligible submission; distinguish completed-but-unWaited refs | CONFIRMED own quota event, not PG queue congestion or device concurrency |
| PREFETCH_TOO_LATE | Linked request/page was observed unfinished when needed; positive submit-to-need interval and known demand classification | CONFIRMED late relative to observed completion; increasing distance as remedy remains SUSPECTED because I/O can itself be slow |
| READSTREAM_RESET_OVERHEAD | Exact repeated creations/resets in the same logical scan lifecycle, measured initialization duration and observed warmup repetition | CONFIRMED occurrence/cost; cannot call separate table scans a reset |
| QUEUE_FULL_FALLBACK | Explicit causal queue-full event correlated with execution, or validated external trace showing the exact provider fallback path and scope | Otherwise INSUFFICIENT_DATA; automatic internal exact count/time remains UNAVAILABLE |
| SYNC_IO_FALLBACK | Directly observed execution branch selecting/performing synchronous reads, not merely a configured mode or f_sync request flag | CONFIRMED only when execution is proven; generic sampled synchronous stack permits occurrence evidence, never exact whole-query count/time |
| BUFFER_PRESSURE_SUSPECTED | Multiple compatible signals: bounded repeated-load evidence plus replacement/eviction deltas, with source/concurrency caveats | SUSPECTED; low hit rate or normal ring reuse alone never fires |
| IO_LATENCY_HIGH | Linked observed-completion latency bounds, not-ready demand and sufficient request samples; compare with measured consumption capability | SUSPECTED unless a narrower measured fact is stated; do not fabricate device service latency |
| IO_REQUEST_TOO_SMALL_SUSPECTED | Actual I/O-request-size evidence excludes ready cache operations, and low throughput or significant CPU/submit cost supports impact | SUSPECTED; accepted_single/requests by itself is insufficient |
| STORAGE_BANDWIDTH_LIMITED | Reliable time-aligned device throughput/utilization plus an independently established sustained-bandwidth reference, while readers are active | SUSPECTED/CONFIRMED only according to evidence; logical PG read bytes are insufficient |
| BUFFER_MANAGER_OVERHEAD | Public API envelope or CPU samples attribute substantial non-wait work to Buffer Manager, excluding submission-sync and unrelated work | SUSPECTED for broad API CPU evidence; exact Buffer Manager exclusive time often UNAVAILABLE |
| INSUFFICIENT_EVIDENCE | Required fields unavailable, missing samples, incompatible scope or contradictory data | INSUFFICIENT_DATA with the missing evidence and next experiment |

When only the current counters are available, the safe useful findings are
own request-quota/pin-observation facts, expensive API envelopes, sampled
CPU-versus-elapsed observations and insufficient-evidence statements for
private PG causes. No rule should invent a complete story to fill the report.

## Specific limits on synchronous-execution diagnosis

`io_method=sync` confirms configuration, not that every submitted page incurred
a synchronous disk read. Another backend can finish the page before Wait.
YAAP currently delegates reads to PG and has no independent physical pread
branch to count. Do not create one solely to make Test F produce a number.
An explicit YAAP branch forcing READ_BUFFERS_SYNCHRONOUSLY can confirm the
request choice, but still cannot establish physical I/O without execution
observability. Keep choice, execution, reason and time separate.

`f_sync` is not a queue-full reason. High submit time with low thread CPU is
also not a queue-full counter: it can include locks, descheduling and storage
work. The previous external worker samples are auxiliary evidence, not a
portable in-extension causal counter. A rule can recommend a correlated
DEBUG/pg_aios/stack experiment while reporting exact fallback fields unavailable.

## Configurable thresholds

Thresholds decide whether an observed condition is worth surfacing; they must
never decide whether a page was ready or whether a wait truly blocked.
Suggested options in a rule-config value object (not necessarily separate
GUCs): minimum page/request/sample count, minimum not-ready fraction, minimum
limit-event fraction, minimum attributed worker-time fraction and maximum
findings. Defaults are diagnostic heuristics and should be printed with the
report or accessible in documentation. Tests should exercise exact boundary
values. Avoid a default latency threshold advertised as a universal SSD limit.

## Contradiction checks before rules

- Counts violate partitions or a numerator exceeds its exact denominator.
- Same clock domain gives submit > need, observation > containing envelope,
  negative delta or apparent CPU time far above wall.
- Exclusive subcomponents exceed Next envelope beyond rounding allowance.
- Primary elapsed source time is greater than its containing worker fragment.
- Histogram totals disagree with observation/sample counts.
- Exact miss count conflicts with an initial-state unknown population.
- Query data is partial or record PID changed without a new identity.
- Configuration/request-quota differs among workers but aggregate presents one
  last-worker value as universal.

Print a DATA_INCONSISTENT/INSUFFICIENT_EVIDENCE finding rather than rescaling
counts or hiding negative residuals. Existing tuple sampling may overestimate
its parent; this is an explicitly estimated-domain inconsistency and must not
invalidate unrelated exact queue event counts.

## Tests for the rule module and integration

Pure deterministic fixtures should cover every rule's positive, negative,
missing-data and exact-threshold cases. In particular:

1. Hot cached API accepted=1 many times: no small-I/O or buffer-pressure claim.
2. Wait calls positive but exact blocked time unavailable: no exact Stall.
3. All Waits ready before invocation: no actual-wait count merely from calls.
4. Independent lead P95 < completion P95 with no paired late events: no late
   inference; linked not-ready observations can support a narrower finding.
5. Ring equals granted window because pin clamp chose both: pin/fair-share
   reason only, no independent ring-pressure claim.
6. Exact request cap stops, low actual wait: confirm cap fact but do not claim
   the cap is critical-path limiting.
7. Low hit rate for a one-pass cold scan: no buffer-pressure finding.
8. Generic f_sync or long submit: fallback exact fields stay unavailable.
9. External synchronous occurrence with no count/time: preserve evidence scope
   and unknown quantitative impact.
10. IO method sync plus all hits: no self-sync physical-read count.
11. Merge 1/4/8/12-worker histograms and weighted ratios; merging does not depend
    on input order and does not average percentiles or percentages.
12. Partial/cancelled query and PID conflict: retain observed facts while
    rejecting full-query ratios and absence claims.
13. Overlapping envelopes, overflow bins, unknown readiness, sample starvation
    and clock contradictions produce explicit insufficiency.

Integration should hold read policy constant while varying cache/pin/worker
conditions. Neither hot nor cold should be judged from an arbitrary elapsed
threshold. Test B/C/E can validate trends, but a trend's exact cause remains a
hypothesis unless directly observed. Measuring profiler overhead requires an
optimized build, interleaved repeated Off/On runs and consistent query results.
