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
kernel stops only after every tagged request completes:

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
validation occupy disjoint deterministic namespaces. Node placement starts only
when every fresh final p95 upper bound meets its SLO.

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

All request classes still run concurrently and share the same FIFO queues and
replicas. A weight controls external traffic mixture only; it does not reserve
capacity or give a class queueing priority. Weights must be finite, positive,
and sum to one.

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
- one independent `std::mt19937_64` stream per replication;
- FIFO waiting queues and fixed per-service replica capacities;
- fixed probabilistic routing, exits, revisits, and loops;
- event ordering by `(time, priority, sequence_number)`;
- internal admission at service completion uses urgent same-time priority;
- external active count changes only at external entry and final exit;
- queue and busy histories record post-transition values and coalesce changes at
  the same timestamp;
- `N_i = Q_i + B_i` and `U_i = B_i / c_i` remain derived quantities;
- fixed-horizon events at exactly the horizon are not processed, matching the
  previous `env.run(until=H)` boundary.

The new random generator is distributionally equivalent but does not reproduce
the old Python `random`/SimPy trajectory bit for bit. Native seeds are stable and
reproducible within this backend.

## Protocol and failures

The adapter sends one JSON request on standard input and expects one JSON
response on standard output. Protocol version 4 supports:

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
equivalence, measurement gates and draining, multiple entry streams, routing
loops, structured failures, and M/M/1 statistical sanity. It is also run against
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
