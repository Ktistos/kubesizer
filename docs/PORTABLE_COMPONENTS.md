# Portable component performance models

These local extensions model declared resource and execution mechanisms. They
do not identify application internals from a product name. GPU execution is
excluded. No new deployment accuracy results accompany this implementation.

## Components represented

| Component | Representation | Evidence or declaration required |
|---|---|---|
| Broker and worker pool | Publisher work, background delivery, bounded FIFO backlog, admitted/prefetched jobs, worker processing slots | Publish/job completion rates, job durations, configured worker concurrency/prefetch, queue capacity, acknowledgement boundary |
| Gateway or proxy | Local processing, bounded admission, queue deadline, held downstream calls | Local CPU demand, configured active/pending limits and timeout, downstream structure |
| Connection or thread pool | Finite concurrency held while a child workflow runs | Configured pool size and actual acquisition/holding boundary; connection counts alone are insufficient |
| Cache | Class-specific fixed hit probability, or shared per-replica LRU/TTL state and conditional miss work | Hit/lookup counters at the observed configuration; key sizes and access profile for stateful cache extrapolation |
| Shared network link | Byte work on one shared processing resource, plus disjoint transit delay | Payload sizes and effective link capacity; one-way transit or properly attributed round-trip measurements |
| External API | Fixed exogenous delay resource, finite connections, per-resource token bucket, failures and bounded retries | Response samples, quota/burst policy, failure counts, deadlines and backoff |
| Serverless instance pool | Declared fixed instance count, admission concurrency, cold startup and idle expiry | Startup samples, configured idle policy, concurrency and placement/routing policy |

Examples are under [`examples/components`](../examples/components). They are
synthetic software examples. Broker persistence/ordering, consumer-group
rebalancing, distributed cache coherence, packet/TCP behavior, provisioning
failure and autoscaling-controller behavior are not implemented by these
abstractions. Existing stateful/storage stage workflows remain available.

## Resource and request boundaries

`traffic_model.service_times` contains physical resource identities. Every
request class using the same identity contends for that same resource pool.
`processing_slots_per_replica` limits simultaneous local execution;
`concurrency_limit_per_replica` limits admitted visits, including visits holding
a slot while waiting. Pending admission is a separate FIFO queue. For example,
two processing slots and eight admitted jobs can represent a worker pool with
prefetched work; this is not a complete broker protocol model.

A `stage_workflow` names phases over these resources. `hold_resource: true`
retains admission across child calls. A `next` continuation runs after the
stage's response requirement succeeds. Failed stages skip their continuation.
Use a separate resource-free `delay` stage for transit; do not count the same
round trip again as service blocking or an external latency residual.

`completion_boundary: client_response` is the default request-class boundary.
`all_work` waits for all descendant work, including background/quorum tails and
old retry attempts. It is useful for job completion SLOs. It selects one latency
boundary per class; it does not report publisher acknowledgement and job
completion as two separate latency cohorts. Root deadlines still produce a
terminal failed outcome at the deadline while server-side work continues.

## Optional resource policies

```yaml
worker:
  distribution: exponential
  rate: 100                  # 10 ms mean local processing work
  processing_slots_per_replica: 2
  concurrency_limit_per_replica: 8
  admission_queue:
    capacity_per_replica: 64  # waiting outside admission; zero means no waiting
    timeout_seconds: 0.2     # zero disables the pending-queue deadline
```

Either admission field can be supplied. Omitted capacity means unbounded pending
capacity. Rejection happens before local work. Queue timeouts remove pending
visits; they do not cancel work already accepted for processing.

`execution_model: delay` samples a wall-time duration without CPU contention.
It defaults to unlimited admitted concurrency; a declared concurrency limit
creates an admission pool. Use it for a fixed external latency or connection
holding phase, not as a substitute for a known shared bottleneck.

```yaml
external_api:
  execution_model: delay
  concurrency_limit_per_replica: 4
  rate_limit: {rate_per_second: 5, burst: 2, action: wait}
  failure_probability: 0.02
```

Token buckets start full, refill continuously and consume one token per admitted
visit. `wait` keeps FIFO order; `reject` fails when a token is unavailable. Each
independent replica queue has its own bucket. A shared third-party quota must
therefore use one fixed resource, not a bucket replicated with application pods.
`failure_probability` is an explicitly declared independent local failure after
local processing; it does not infer load-dependent failure from CPU telemetry.

```yaml
lifecycle:
  startup_delay: {distribution: deterministic, mean: 0.12}
  idle_timeout_seconds: 30
  initially_warm: false
```

Lifecycle policies require independent replica queues. Accepted work retains
admission during startup. Idle expiry occurs with no admitted/pending work and
clears volatile cache state. Late miss results cannot repopulate a destroyed
instance. The replica count is fixed within a candidate run: no HPA/KPA loop or
scale-to-zero controller is inferred. Startup and idle expiry do not imply cache
coherence or persistence.

## Retry controls

```yaml
invoke:
  calls: {attempt: 1}
  retry:
    max_attempts: 3
    attempt_timeout_seconds: 0.1
    backoff: {distribution: deterministic, mean: 0.01}
```

The retry wrapper is resource-free with exactly one child and all-response
completion. `max_attempts` includes the first attempt and is bounded at 100.
Backoff is optional; zero attempt timeout disables that deadline. A local failure
or deadline can initiate another attempt. Timed-out attempts keep their CPU,
admission and downstream work. Their late results cannot replace the current
attempt's outcome. Retried operations must be safe to repeat according to the
application; the model does not implement idempotency or transactional effects.

Potential rejection/failure/deadline workloads with latency SLOs must also declare
`maximum_terminal_failure_rate`. This prevents quick rejections from appearing
to satisfy a latency-only recommendation. Diagnostic raw simulation can omit
SLOs. Failure budgets are checked alongside latency confidence intervals.

## Cache evidence levels

A resource policy `cache.mode: fixed_hit_ratio` requires
`hit_ratio_by_class`, `capacity_bytes_per_replica`,
`observed_capacity_bytes_per_replica` and `observed_replicas`. The capacities
must agree; the parser fixes the cache replica count to the observed count and
rejects conflicting candidate ranges. This represents the observed mixture,
not a prediction of hit rate at a different capacity. Combining this fixed
mixture with cold instance lifecycle is rejected.

`cache.mode: lru` instead requires `capacity_bytes_per_replica`,
`key_sizes_bytes` and `accesses_by_class`. `ttl_seconds: 0` disables expiry.
Each class supplies `{sampling: iid, keys: [...]}` or
`{sampling: cyclic_trace, keys: [...]}`. Repeated keys in IID profiles encode
frequency. A cyclic trace preserves key order and repeats it; it does not
preserve arbitrary key/time correlations. Keys are sampled once per external
request/resource and reused by that request's cache stages. TTL starts at fill;
hits update LRU order without extending TTL. Oversized items are not retained.

LRU requires `replica_queue_model: independent_key_hash`: a stable 64-bit FNV-1a
hash of the key name modulo replicas selects the shard. This explicit policy
must match the deployment or be treated as an approximation. It is not a claim
to reproduce Redis Cluster or another product's hash slots/resharding behavior.
Caches are shared across classes. Simultaneous misses can duplicate downstream
work; there is no implicit single-flight coalescing.

Cache resource stages declare `cache: {miss: load_stage}` and optionally `hit`
and `fill_on_miss: false`. Only the selected branch runs. A successful miss fills
the cache; a failed miss does not. `operation: put` and `operation: invalidate`
have no branches. All operations pay their own declared local work. Every use of
a cache resource requires explicit cache-operation stages; metadata cannot be
silently attached to an ordinary queue.

## Portable measurement input

The existing standard service measurement workflow is unchanged. Additional
component evidence is optional and explicitly mapped in a workload's
`component_measurements` module. Only sources referenced by estimates used in
that workload are read. This module does not collect data from a live cluster.

See [`evidence-worker.yaml`](../examples/components/evidence-worker.yaml) for a
complete runnable example. It resolves a typed reference:

```yaml
mean: {measurement: cpu_demand, unit: seconds}
```

The module requires `version: 1`, `sources` and `estimates`. Source adapters are:

- `csv`: numeric observations with the exact header
  `metric,component,request_class,window_start,window_end,value,unit,boundary,kind`.
  `kind` is `total` or a per-operation `sample`. Counter totals must already be
  summed across replicas within each retained window; duplicate/overlapping
  windows are rejected.
- `prometheus`: a saved successful instant-query vector plus explicit `query`,
  `labels`, and all numeric observation metadata except `value`. Matching pod
  series are summed. Save a query for window totals, such as `increase(...)`,
  evaluated at `window_end`. The adapter cannot prove the recorded query's
  application-level attribution or convert an instantaneous gauge into demand.
- `otlp_spans`: decoded OTLP JSON `resourceSpans/scopeSpans/spans`, explicit
  string-attribute filters and numeric metadata with `kind: sample` and
  `unit: seconds`. It produces span wall durations, never CPU demand. Selected
  spans must fit the retained window. Nested/inclusive spans must be separated
  before treating them as local blocking phases.
- `cache_access_csv`: header
  `component,request_class,window_start,window_end,timestamp,key,size_bytes`.
  It retains key order and consistent positive object sizes. Hash/anonymize keys
  consistently before export if needed.

All windows are explicit UTC intervals with positive duration. `total`, `rate`,
`sample_mean` and `samples` estimates use a `select` mapping containing
`source,metric,component,request_class,unit,boundary`. A rate divides aggregate
totals by the sum of non-overlapping window durations. A `samples` estimate
creates an empirical IID duration distribution from per-operation samples;
window means are not accepted as per-operation samples by declaration.

`transfer_samples` selects per-operation `bytes` samples and divides each by a
declared positive `capacity_bytes_per_second`, producing an empirical duration
distribution in seconds for shared link work. This preserves payload-size
variation. Use a byte-work stage with `unit: bytes`, `work_units: 1` and the
result as `service_time`; keep transit delay separate. Observed average traffic
is not assumed to be available link capacity. Independent empirical sampling
does not preserve cross-phase payload correlations.

`ratio` uses `numerator` and `denominator` selectors with aligned windows,
component and class. Supported unit pairs are CPU-seconds/requests or
response-seconds/requests → seconds, bytes/requests → bytes, and
requests/requests → probability. Sum counters first, then divide: this weights
replicas/windows by requests instead of averaging per-replica ratios. A cache
hit ratio uses hit count divided by lookup count. CPU counters must be attributed
to the same work/class; unrelated background CPU and unfinished work can still
make a syntactically valid estimate inappropriate.
Distribution shape remains declared unless genuine per-operation samples are
provided; variation between window means is not fitted as per-request variance.
Observed completion throughput is also rejected as a processing service rate.
Use the measured CPU demand as a mean with an explicitly declared shape (the
worker example uses gamma shape one, equivalent to an exponential duration).

`key_sizes` and `key_accesses` estimates use a key source, component and class;
their units are `key_sizes_bytes` and `key_sequence`. The catalog selector can
use class `*` to combine consistent sizes. The chosen sampling mode remains an
explicit declaration.

Consumed source hashes, adapter mappings and resolved estimates are available
through `System.get_component_measurement_provenance()` and included in
calibration identity/reports. Changed evidence invalidates old calibration.
Configured concurrency and replica bounds must be declared, not inferred from
maximum observed active requests. Aggregate Istio/Prometheus counters do not
uniquely identify pool holding, cache reuse, execution phases or retry behavior;
use suitable optional exporter/OTLP evidence or omit those semantics.

## Analysis, optimization and verification

Static compatible stages continue to use the existing enriched analytical
response-bound method. Shared processor-sharing link work and independent
unlimited delay have compatible analytical forms under their usual assumptions.
Finite held pools, all-work completion, admission/rejection policies, retry
controls, stateful caches and cold-start lifecycles cause analytical abstention;
no closed-form equivalent is claimed for them.

For conditional/resource-policy models, `solve().get_stability()` is `None`.
Projected service rates, visits and utilization are structural diagnostics, not
certified actual traffic or a necessary capacity floor. The simulation search
starts from declared minima, retains exact candidate enumeration and requires
warm-up/trajectory checks, failure budgets and fresh-seed final validation.
Bounded queues can be stationary while rejecting unacceptable amounts of work.
A finite cohort completing is not proof of steady-state stability.

Use the normal interface, without new subcommands:

```bash
kubesizer --workload examples/components/evidence-worker.yaml --replica-evaluation simulation --json
```

Local software checks cover deterministic timing, conditional work, ownership
after deadlines, evidence validation and the calibration/search/final-validation
path. They establish implementation behavior only. Controlled characterization
and holdout deployment experiments, including overload and changes to capacity,
are the next stage; the new semantics have not yet been validated on arbitrary
clusters or on the existing frozen VM campaigns.
