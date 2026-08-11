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

## Frozen execution semantics

- exponential external interarrival and service times;
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
response on standard output. Protocol version 2 supports:

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
