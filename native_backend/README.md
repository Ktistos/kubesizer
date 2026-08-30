# Kubesizer native simulation backend

Kubesizer uses this C++ event kernel as its required simulation executor.
Python still owns workload construction, analytical stability checks, warm-up
cutoff decisions, optimization, and reporting. There is no SimPy runtime
fallback and no backend-selection flag.

## Build

```bash
native_backend/cpp/build.sh
```

The build requires a C++20 compiler, CMake, and Jansson. Set
`KUBESIZER_NATIVE_PREFIX` when these are installed under a non-system prefix.
The default executable is:

```text
native_backend/cpp/build-release/kubesizer-simulator
```

`KUBESIZER_CPP_BACKEND` may point to another build of the same protocol version.
This changes the binary location, not the execution architecture.

For a sanitizer build:

```bash
native_backend/cpp/build.sh --sanitizers
```

## Use from Python

The existing fixed-horizon API now executes in C++:

```python
metrics = system.simulate(
    1_000.0,
    warmup_duration=100.0,
    seed=42,
)
```

Pass a collector when full event-driven state histories are required:

```python
from metrics import SimulationMetricsCollector

collector = SimulationMetricsCollector(
    len(system.get_services()),
    latency_collection_start=100.0,
)
metrics = system.simulate(
    1_000.0,
    warmup_duration=100.0,
    metrics_collector=collector,
    seed=42,
)
active_history = collector.get_active_history()
queue_history = collector.get_queue_history()
busy_history = collector.get_busy_history()
```

For statistically independent p95 replications, use the uncensored measurement
API. Arrivals in `[W, W + M)` are tagged, background traffic continues, and the
kernel stops only after every tagged request reaches a terminal client outcome:

```python
replications = system.simulate_measurement_replications(
    warmup_duration=100.0,
    measurement_duration=1_000.0,
    seeds=tuple(range(300_000, 300_030)),
    workers=8,
)

for replication in replications:
    assert (
        replication.measured_request_counts
        == replication.completed_measured_request_counts
    )
    print(replication.seed, replication.request_class_p95_latencies)
```

An optional request-class `end_to_end_timeout_seconds` models a measured client
or gateway deadline. A timeout is retained in the latency cohort at the exact
deadline and marked in `request_class_failures`; it is never discarded as a
fast-looking censored request. Server-side work continues until it completes,
so final active/queue diagnostics can remain nonzero after every tagged client
outcome has terminated. With no declared deadline, behavior is unchanged and
all failure flags are false.

## Statistically controlled optimizer evaluation

The production workflow is exposed by `statistical_workflow.py` and described
in `STATISTICAL_WORKFLOW.md`. Python calibrates and freezes global warm-up `W`,
measurement duration `M`, sequential replication limits and seed namespaces.
For every analytically stable configuration the native backend returns the raw,
separate request-class latency clusters and arrival timestamps required for:

- pooled request-class p95 point estimates;
- replication-cluster percentile-bootstrap confidence intervals;
- conservative Bonferroni control across both tails, sequential looks and
  request classes;
- distinct feasible, infeasible, inconclusive, analytical-failure and
  simulation-failure results;
- fixed fresh-seed final validation without adaptive early stopping.

Common random numbers use the same simulation seed for replication index `r`
across configurations. Calibration, optimization, bootstrap and final
validation occupy disjoint deterministic namespaces. When resources are
supplied, node placement runs before fresh final validation so the final p95
bounds include directional network delay and explicitly measured node slowdown.

Aligned measurement-window arrival and CPU-demand multipliers are sampled
jointly with a moving-block bootstrap once per replication. Raw arrival
timestamp ordering is preserved separately by the trace-block sampler. These
input sources remain distinct from Monte Carlo error and model-form assumptions.

The C++ process owns stochastic event execution, request tagging, cohort
draining, state integration and replication parallelism. Python owns the
statistical design, fingerprints, calibration decisions, classification,
search, validation and reporting.

The calibration workflow uses `System.run_warmup_ensemble()` internally. Each
replication is independent, C++ runs a bounded native worker pool, bucket areas
are integrated online, and corresponding buckets are reduced in seed order.
Only ensemble means and sample variances cross the process boundary. A
measurement replication additionally returns raw class latency and arrival-time
arrays so shorter valid arrival cohorts can be derived without rerunning the
kernel.

## Weighted concurrent request classes

The current workload format defines exactly one `traffic_model`. There is no
scenario collection or redundant name such as `normal`; to evaluate another
operating condition, change the model's distributions and rerun calibration
and optimization. Numeric rates, means, horizons, and SLOs use seconds by
convention.

The weighted workload schema is:

```yaml
request_classes:
  read_heavy:
    concurrency_weight: 0.7
    routing: {}
  write_heavy:
    concurrency_weight: 0.3
    routing: {}
traffic_model:
  entry_services:
    read_heavy:
      api: {distribution: exponential, rate: 7.0}
    write_heavy:
      api: {distribution: exponential, rate: 3.0}
  service_times:
    api: {distribution: exponential, rate: 100.0}
```

All request classes still run concurrently and contend for the same service
capacity. A weight controls external traffic mixture only; it does not reserve
capacity or give a class queueing priority. Weights must be finite, positive,
and sum to one.

## Measured latency boundaries and network transit

New workloads should name non-service latency instead of fitting one anonymous
constant. `external_client` includes the load-generator/client boundary;
`ingress` begins at the ingress controller. `client_to_entry` is the portable
aggregate fallback when ingress telemetry is unavailable and cannot be combined
with the finer ingress components.

```yaml
request_classes:
  read:
    concurrency_weight: 1.0
    latency_boundary: external_client
    latency_components:
      client_to_ingress: {distribution: deterministic, mean: 0.028}
      ingress_processing: {distribution: deterministic, mean: 0.0004}
      unmodeled_residual: {distribution: deterministic, mean: 0.006}
    routing: {}
```

For service calls, HTTP round-trip probes between application pods can be
normalized to one-way same-node and cross-node series. Before placement is
known, the measured/estimated cross-node call fraction selects between them.
After placement is attached, its explicit directional matrix is authoritative.

```yaml
traffic_model:
  communication_latency:
    same_node_one_way: {distribution: deterministic, mean: 0.0011}
    cross_node_one_way: {distribution: deterministic, mean: 0.0015}
    unplaced_cross_node_probability: 0.6
```

Synchronous request/response pairs reuse the same sampled unplaced probe path,
preserving the empirical round-trip distribution. Analytical mean evaluation
uses the component means and expected call round trips; simulation samples the
full component distributions. Mean differences may be derived from aligned
counter windows, but independently aggregated percentiles must never be
subtracted to manufacture a tail distribution.

## Replica queue dispatch

The service-time declaration can select how a fixed-slot service dispatches a
visit across replicas:

```yaml
traffic_model:
  service_times:
    api:
      execution_model: fixed_slots
      replica_queue_model: independent_round_robin
      distribution: lognormal
      mean: 0.025
      scv: 2.0
```

`independent_round_robin` is the default and models a load balancer assigning
successive visits to separate per-replica queues. `independent_random`
independently selects a replica for every visit. `pooled` represents one ideal
central queue feeding all replica capacity and must be requested explicitly.
The selected replica owns that visit's admission queue, concurrency capacity,
processing queue or processor-sharing scheduler, and placement node until the
visit completes.

Independent queues are often the more faithful model for Kubernetes pods. They
can be slower than a pooled queue because an idle pod cannot take a request
already waiting behind work at another pod. The dispatch policy must match the
deployment's actual load-balancing behavior; it is a model input, not something
the simulator can infer from aggregate latency measurements. Both fixed-slot
and processor-sharing execution support pooled and independent dispatch.

## Per-visit non-CPU blocking

An optional service-time field represents a measured wait which is part of the
service visit but does not consume CPU processing capacity:

```yaml
traffic_model:
  service_times:
    api:
      execution_model: fixed_slots
      concurrency_limit_per_replica: 32
      non_cpu_blocking_time:
        distribution: lognormal
        mean: 0.006
        scv: 1.5
      distribution: gamma
      mean: 0.004
      scv: 0.8
```

The CPU phase runs first. Its processing slot is then released while the visit
waits for `non_cpu_blocking_time`; the admitted concurrency slot is retained
until routing or synchronous child-call processing continues. This models
I/O, locks, connection-pool waits, or external waits without charging them as
CPU demand. It can also be declared per request class. The values must exclude
downstream and network latency already represented by the call graph and
communication model.

This semantic is simulation-only for certification. The analytical mean
diagnostic includes its mean, but strict analytical optimization rejects such
a workload because finite-concurrency blocking is not represented by the
M/M/c, M/G/1, or variability-adjusted queueing equations.

Let `G` be the sum of all configured external `gamma` values, `w_k` the weight
of class `k`, and `G_k_raw` that class's configured external total. The class
target and per-entry effective rates are:

```text
G_k = w_k * G
gamma_effective[k,i] = G_k * gamma_configured[k,i] / G_k_raw
```

Thus weighting preserves both the total offered traffic and the relative split
between multiple entry services inside one class. The repository workload uses
weights 0.7, 0.2, and 0.1, so its original total rate of 33 becomes class rates
23.1, 6.6, and 3.3.

Every request class must declare a concurrency weight, and the weights must sum
to one. Legacy schema-v2 files and in-code workload dictionaries remain
read-compatible; new workload files use `request_classes` and `traffic_model`
without schema metadata.

## Custom distribution plugins

Custom arrival and service-time samplers may be supplied as trusted native
shared libraries. The public C ABI is declared in
`cpp/include/kubesizer/custom_distribution.h`. Build the bundled example and
inspect `examples/workloads/custom-distribution.example.yaml`:

```bash
native_backend/cpp/build.sh
```

A custom distribution declares `library`, `symbol`, `abi_version`, numeric
`parameters`, and analytical `scv`. Interarrival distributions declare `rate`;
service-time distributions declare `mean`. A sampler receives a callback to
the replication's deterministic random stream and returns a positive, finite,
dimensionless multiplier with expected mean one. The kernel multiplies that
value by the declared mean. Plugins must be reentrant, thread-safe, must not
retain callback state, and must not throw exceptions across the C ABI.

The parser resolves the library path and fingerprints its SHA-256 content, so
changing the plugin invalidates statistical calibration. The kernel verifies
the ABI version and exported sampler symbol when loading the plugin.

Custom plugins execute native code with the user's permissions inside the
simulator subprocess. Workload files must therefore reference only trusted
libraries. Declared means and SCVs are inputs to analytical approximations and
are not proof that a plugin actually produces those moments; validate custom
samplers statistically before relying on analytical optimization.

## Frozen execution semantics

- exponential, deterministic, gamma, lognormal, Weibull, empirical, trace, and
  trusted custom-plugin sampling as applicable to arrivals and service times;
- request-class-specific service-demand distributions;
- one independent `std::mt19937_64` stream per replication;
- aligned arrival-rate and service-demand epochs sampled jointly with a
  circular moving-block bootstrap once per replication;
- trace-block external arrivals that preserve measured local burst ordering;
- separate processing-slot and admitted-request-concurrency capacities, both of
  which scale with replica count;
- pooled or independently dispatched FIFO admission and processing queues for
  fixed-slot services;
- fixed probabilistic routing, exits, revisits, and loops;
- optional fixed node placement with directional one-way transit delays and
  measured node execution-time multipliers;
- ordered workflows for simulation-ready sequential visit models;
- synchronous call graphs with an explicit per-class `call_execution` mode:
  sibling calls run either in declared sequence or in parallel, while parents
  retain concurrency and release processing capacity as they wait;
- event ordering by `(time, priority, sequence_number)`;
- internal admission at service completion uses urgent same-time priority;
- external active count changes only at external entry and final exit;
- queue and busy histories record post-transition values and coalesce changes at
  the same timestamp;
- `N_i = Q_i + B_i` and `U_i = B_i / c_i` remain derived quantities;
- fixed-horizon events at exactly the horizon are not processed, so the run
  covers the half-open interval `[0, H)`.

The new random generator is distributionally equivalent but does not reproduce
the old Python `random`/SimPy trajectory bit for bit. Native seeds are stable and
reproducible within this backend.

The call-graph and dual-resource fields are illustrated in
`examples/workloads/synchronous-call-graph.example.yaml`. A `visits` workload
remains an analytical mean-flow input only: edge counters do not reveal whether
calls were sequential, parallel, synchronous, or asynchronous, so Kubesizer
does not invent executable timing semantics from visit ratios.

For `call_execution: sequential`, child services execute in the mapping order
written under each source. For `call_execution: parallel`, all sampled sibling
calls start at the same simulation timestamp and the parent resumes only after
the last child subtree completes.

Processing service time should represent exclusive demand for the modeled
processing resource, such as CPU-seconds/request divided by the declared CPU
capacity. Inclusive Istio response time must not be used as CPU demand because
it can contain downstream waits that the call graph already models. The
concurrency limit represents worker/thread/in-flight capacity and must come
from deployment configuration or a controlled measurement; it is not inferred
from CPU demand.

## Protocol and failures

The adapter sends one JSON request on standard input and expects one JSON
response on standard output. Protocol version 12 supports:

- `simulate`;
- `warmup_ensemble`;
- `measurement_replications`.

Invalid input and kernel invariant failures return a structured error and a
non-zero exit code. Python raises `NativeBackendError`. A missing binary is a
hard error; Kubesizer never falls back to a slower simulator.

## Tests

```bash
pytest -q native_backend/tests/test_native_backend.py
pytest -q
```

The backend suite covers reproducibility, state invariants, online bucket
equivalence, measurement gates and draining, client deadlines, multiple entry
streams, routing loops, structured failures, and M/M/1 statistical sanity. It is also run against
the ASan/UBSan build during validation.

## Analytical consistency validation

Run the dedicated native-versus-analytical matrix with:

```bash
pytest -q tests/test_native_analytical_consistency.py
```

Every case uses 30 independent native replications with fixed, disjoint seeds.
The matrix covers M/M/1 at 25%, 50%, 75%, 90%, and 95% utilization, M/M/2 at
60%, M/M/3 at 85%, a multiclass feed-forward network with multiple external
entry streams, and a feedback network with repeated visits.

The checks compare the native results with the independent analytical traffic
and M/M/c calculations for:

- external-arrival and completed-request throughput;
- per-service utilization;
- time-weighted queue length;
- time-weighted number in system using Little's law;
- single-service mean response time;
- routed request-class mean end-to-end latency using analytical visit ratios;
- M/M/1 p95 against its closed-form distribution in the backend sanity suite.

Mean-latency agreement treats complete replications as the independent units
and accepts a difference no larger than four estimated standard errors or a 3%
practical-error floor. State checks use documented absolute/relative floors.
Late state buckets exclude the empty-start half of each pilot, and the 95%
utilization case deliberately uses a longer horizon because its queue mixes
slowly.

This is stochastic regression evidence, not a mathematical proof of the kernel.
It applies to the stationary exponential, FIFO, fixed-routing model implemented
here. Arbitrary-network p95 has no corresponding closed form in the analytical
model and is validated through independent simulation replications instead.
