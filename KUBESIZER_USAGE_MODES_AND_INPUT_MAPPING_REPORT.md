# Kubesizer Usage Modes, Optimization Flows, and Input Mapping

**Repository:** Kubesizer  
**Audited branch:** `feature/statistical-simulation-workflow`  
**Audited revision:** `6d223c1` plus the uncommitted statistical-stability-floor integration present on 31 August 2026  
**Report date:** 31 August 2026  
**Purpose:** Explain how to run the current prototype, which optimization flows it supports, which data a DevOps engineer supplies, and what each input means inside the model.

---

## 1. Executive summary

Kubesizer answers one practical question:

> For this measured workload and this declared latency target, how many replicas should each service start with, and—if a node file is supplied—where should those replicas be placed?

The current command is simply:

```bash
kubesizer [options]
```

There are no subcommands. One invocation can perform:

1. workload construction;
2. a conservative starting-capacity calculation;
3. optional statistical simulation calibration;
4. replica-sizing search;
5. optional node-placement search;
6. final fresh-seed validation in simulation mode; and
7. a compact per-service recommendation.

The most important choice is the **input path**:

| Input path | Supplied files | Starting lower bound | Best use |
|---|---|---|---|
| **Portable evidence path** | `workload.yaml` with service declarations + measurement manifest and CSV files | Maximum of the analytical floor and a confidence-bounded empirical capacity floor | Recommended when real deployment measurements and capacity runs exist |
| **Direct model path** | A self-contained `workload.yaml` | Analytical queue-stability floor only | Synthetic studies, prototypes, or manually characterized workloads |

The next choice is the **replica evaluator**:

| Evaluator | What it decides | Strength | Restriction |
|---|---|---|---|
| `analytical` | Analytical stability and optional **mean** latency SLO | Fast | Does not certify p95, timeouts, or rich blocking/concurrency semantics |
| `simulation` | Mean or p95 latency using independent C++ simulation replications and confidence intervals | More expressive and statistically controlled | More expensive; only as realistic as the supplied distributions and latency components |

The final choices are independent search methods:

| Search stage | `exact` | `scalable` | `auto` |
|---|---|---|---|
| Replica sizing | Uniform-cost search in the bounded replica space | Randomized greedy construction, deletion descent, multi-start VNS | Exact up to 10,000 bounded configurations; scalable above that |
| Node placement | Branch-and-bound | Diversity-aware beam search plus relocation, swaps, and LNS/VNS | Exact up to 1,000,000 estimated placements; scalable above that |

“Exact” describes search coverage **inside the chosen model and bounds**. It does not turn an approximate analytical latency model into physical truth. “Scalable” is a heuristic and carries no general global-optimality guarantee.

For the strongest current evidence flow, use the portable path with simulation:

```text
measured workload windows + independent capacity runs
                         |
                         v
confidence-bounded stable replica floor
                         |
                         v
calibrated C++ simulation and SLO search at or above that floor
                         |
                         v
optional node placement and placement-aware recalibration
                         |
                         v
fresh-seed final validation
```

This result is conservative **conditional on the measured workload envelope and the capacity-scaling assumptions**. It is not a guarantee against arbitrary future traffic, a changed deployment, or an unmeasured shared bottleneck.

---

## 2. Installation and command shape

From the repository root:

```bash
make native-backend
make install-cli
kubesizer --help
```

`make native-backend` builds the required C++ event simulator. Simulation is not an optional Python fallback: `--replica-evaluation simulation` requires the native backend.

The important arguments are:

```text
--workload PATH
--measurements PATH
--resources PATH
--skip-placement
--max-replicas N
--workers N
--replica-evaluation analytical|simulation
--replica-search auto|exact|scalable
--placement-search auto|exact|scalable
--json
--out PATH
--quiet
```

Defaults relevant to normal use are:

| Option | Default |
|---|---:|
| Workload | `workload.yaml` |
| Replica evaluation | `simulation` |
| Replica search | `auto` |
| Placement search | `auto` |
| Per-service upper bound | 10 replicas |
| Workers | Up to 8, limited by detected CPU count |
| Result files | None unless `--out` is supplied |

Progress is concise and goes to standard error. The final human-readable recommendation goes to standard output. `--json` emits one stable JSON envelope for automation. `--quiet` suppresses progress. The runtime estimator and elapsed-time progress reporting remain part of the command path; they do not change the optimization result.

### 2.1 Minimal direct-model run

```bash
kubesizer \
  --workload workload.minimum.yaml \
  --replica-evaluation analytical \
  --replica-search exact \
  --skip-placement
```

### 2.2 Measured-evidence run with simulation

```bash
kubesizer \
  --workload examples/portable_evidence/workload.yaml \
  --measurements examples/portable_evidence/measurements.with-capacity.yaml \
  --replica-evaluation simulation \
  --replica-search auto \
  --skip-placement
```

### 2.3 Full run with node placement

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --resources resources.yaml \
  --replica-evaluation simulation \
  --replica-search scalable \
  --placement-search exact
```

This last command is a valid **hybrid search workflow**: scalable replica sizing and exact placement. There is no separate `--hybrid` flag because the two search methods are selected independently.

---

## 3. The two input paths

## 3.1 Portable evidence path: recommended for measured deployments

This path is selected when `--measurements` is supplied. The referenced workload must then contain the `services` mapping needed to interpret that evidence.

The two main files have separate responsibilities:

| File | Owner | Question answered |
|---|---|---|
| `workload.yaml` | DevOps/application owner | Which services and request classes exist, what measured execution context applies, where requests enter, what calls they make, and what SLO must they satisfy? |
| `measurements.yaml` | Measurement exporter | Where are the normalized, retained measurement CSV files? |

The workload rates are **not duplicated** in portable `workload.yaml`. Kubesizer derives them from the measured external intervals. This avoids two competing sources of truth.

Current execution:

1. `characterization.parser.load_portable_sizing_inputs` parses the workload and measurement manifest; service facts are read from `workload.services`.
2. `characterization.analysis.characterize_deployment` converts aligned counters and histograms into arrival, visit, demand, latency, error, and capacity evidence.
3. `characterization.simulation.build_characterized_system_variants` creates the queueing/simulation workload.
4. The CLI requires one unambiguous execution variant. If call ordering is unclear, it asks for `call_execution` rather than silently choosing sequential or parallel semantics.
5. `characterization.stability.estimate_statistical_stability_floor` calculates a joint-confidence lower replica vector.
6. That vector becomes both the hard lower bound and the construction start for replica optimization.

This path therefore separates two decisions:

- **Capacity stability:** Is offered traffic below a confidence-bounded healthy capacity?
- **SLO suitability:** Does the analytical or simulated latency meet the request-class SLO?

The optimizer is never allowed to remove replicas below the empirical stability floor.

## 3.2 Direct model path: self-contained workload

When `--measurements` is omitted, Kubesizer loads a native self-contained workload using `performance_models.system.System`.

The native file directly declares:

- request classes and SLOs;
- external arrival streams;
- service-time distributions;
- routing, workflows, visits, or call graphs;
- optional non-CPU blocking;
- optional communication latency;
- optional residual latency components; and
- optional joint input uncertainty.

The lower bound is `System.get_minimum_stable_replicas()`, derived from mean traffic and service capacity. This path does **not** have the portable flow's confidence-bounded empirical capacity floor. It can still use calibrated statistical simulation for latency, but that simulation certifies the declared model, not the physical deployment.

Use the direct path when the workload is synthetic, when inputs were characterized elsewhere, or when the purpose is model exploration. Do not describe it as deployment-measured stability evidence unless the declared inputs were independently justified.

---

## 4. Supported optimization flows

The replica evaluator and the replica search are independent. Node placement has only a search choice because its current objective is deterministic/analytical.

## 4.1 Flow matrix

| Input path | Replica evaluator | Replica search | Placement search | Supported? | Result meaning |
|---|---|---|---|---|---|
| Portable evidence | Simulation | Exact | Omitted | Yes | Minimum-cost statistically feasible configuration within bounds, subject to unresolved-state caveats, at or above empirical floor |
| Portable evidence | Simulation | Scalable | Omitted | Yes | Certified feasible heuristic incumbent at or above empirical floor; no global guarantee |
| Portable evidence | Simulation | Auto | Auto/omitted | Yes | Default evidence-driven workflow |
| Portable evidence | Analytical | Exact | Exact/scalable/omitted | Yes for compatible mean-SLO models | Exact bounded search under analytical mean model, at or above empirical floor |
| Portable evidence | Analytical | Scalable | Exact/scalable/omitted | Yes for compatible mean-SLO models | Heuristic analytical recommendation, at or above empirical floor |
| Direct model | Simulation | Exact/scalable/auto | Exact/scalable/omitted | Yes | Statistical latency decision for the declared model; analytical stability floor only |
| Direct model | Analytical | Exact/scalable/auto | Exact/scalable/omitted | Yes for compatible mean-SLO models | Fast model-based sizing; no p95 confidence statement |
| Either | Analytical | Any | Any | No for p95 SLO | Percentile SLOs require simulation |
| Either | Analytical | Any | Any | No for terminal timeout/failure semantics | These require simulation |

## 4.2 Analytical replica evaluation

Select with:

```bash
--replica-evaluation analytical
```

The analytical path:

1. solves traffic rates;
2. rejects analytically unstable configurations;
3. automatically chooses a per-service mean queue approximation;
4. computes request-class mean end-to-end latency; and
5. compares it with optional mean latency SLOs.

Queue models are selected in `performance_models.analytical.select_queueing_model`:

| Arrival/service case | Model used |
|---|---|
| Poisson arrivals, exponential service | Exact M/M/c mean via Erlang C |
| Poisson arrivals, general service, one server | Exact M/G/1 mean via Pollaczek–Khinchine |
| Poisson arrivals, general service, multiple servers | Variability-adjusted M/G/c approximation |
| General arrivals, exponential service | Variability-adjusted G/M/c approximation |
| General arrivals, general service | Variability-adjusted G/G/c approximation |

For the general cases, queue waiting from an M/M/c system is multiplied by a variability factor based on arrival and service squared coefficients of variation. This is a useful approximation, not an exact tail model.

Analytical mode is appropriate for quick mean-latency sizing when its assumptions match the workload. It rejects semantics it cannot certify, including finite parent concurrency held during downstream calls and modeled non-CPU blocking that retains an admission slot.

It does not provide:

- p95 confidence intervals;
- terminal timeout/failure-rate modelling;
- warm-up calibration;
- statistical validation across replications; or
- protection against a wrong deployment capacity model unless the portable empirical floor is also used.

## 4.3 Simulation replica evaluation

Select with:

```bash
--replica-evaluation simulation
```

Simulation uses the required C++ discrete-event backend through `native_backend.backend.CppSimulationBackend`. Each replication has independent random streams. The kernel models:

- concurrent external streams for all request classes;
- supported arrival distributions and arrival traces;
- supported service-time distributions;
- routing loops and repeated visits;
- sequential or parallel child calls;
- pooled or per-replica queues;
- FIFO service for fixed slots;
- processor sharing when declared;
- per-replica admission/concurrency limits;
- non-CPU blocking that releases CPU but retains admission;
- same-node/cross-node communication delay;
- placement-specific node execution multipliers;
- end-to-end residual latency components;
- timeouts and terminal failures; and
- event-driven active, queue, and busy histories.

The statistical flow is:

1. Calibrate a global warm-up duration `W` from state histories.
2. Calibrate a post-warm-up measurement-arrival duration `M` from latency precision.
3. For each candidate, start with 30 replications by default.
4. Tag requests arriving in `[W, W + M)`.
5. Continue the run until every tagged request completes, avoiding right-censoring.
6. Pool request latencies for the point mean or p95.
7. Resample complete replication clusters for confidence intervals.
8. Add replications in batches until feasible, infeasible, or the maximum is reached.
9. Reject `INCONCLUSIVE` as an optimizer goal.
10. Validate the selected candidate using a fresh seed namespace and a fixed holdout replication count.

Default statistical settings in `calibration.profile.CalibrationOptions` include:

| Setting | Default |
|---|---:|
| Warm-up pilot replications | 30 |
| Warm-up validation replications | 100 |
| Measurement calibration replications | 30 |
| Minimum optimization replications | 30 |
| Maximum optimization replications | 60 |
| Batch size | 10 |
| Overall alpha | 0.05 |
| Bootstrap resamples | 5,000 |
| Minimum eligible requests per class | 20,000 |
| Final validation replications | 30 |

The sequential confidence allocation is Bonferroni across two tails, planned looks, and request classes. Final validation uses a fresh fixed experiment with familywise adjustment across classes.

These defaults are intentionally demanding. The CLI exposes overrides, but reducing them weakens precision and may invalidate tail resolution.

## 4.4 Exact replica search

Select with:

```bash
--replica-search exact
```

The current exact method is uniform-cost search (`optimization.uniform_cost.UniformCostReplicaOptimizer`). It explores configurations by increasing replica cost, never below the lower-bound vector and never above `--max-replicas`.

If it completes correctly without unresolved candidates preventing proof, it can establish minimum weighted replica cost **within the configured bounded search space and chosen evaluator**. It does not establish that the model matches production.

Simulation makes each configuration expensive, so exact mode can become impractical long before the configuration count looks large.

## 4.5 Scalable replica search

Select with:

```bash
--replica-search scalable
```

The current heuristic (`optimization.scalable_replica.MultiStartReplicaVNSOptimizer`) performs:

1. construction from the stable lower bound;
2. randomized selection among highly ranked single-replica additions;
3. certified-feasible deletion descent;
4. add-and-reprune variable-neighborhood search; and
5. multiple randomized starts within budgets.

An analytical heuristic can rank additions, but only the selected evaluator certifies feasibility. Unknown simulation results are not silently accepted.

The result may be deletion-local when a complete removal pass proves every one-replica removal unacceptable. It is still a heuristic result: it may miss a cheaper configuration requiring a multi-service exchange.

## 4.6 Automatic replica search

Select with:

```bash
--replica-search auto
```

Kubesizer estimates the bounded count:

```text
product over services of (maximum replicas - minimum replicas + 1)
```

It selects exact at 10,000 configurations or fewer and scalable above 10,000. The reason is returned internally and shown in progress metadata. This threshold controls search method, not wall-clock time; a 10,000-state simulation search can still be expensive.

## 4.7 Node placement

Placement is enabled only when a readable `--resources` file is supplied and `--skip-placement` is absent. If the file is omitted or unavailable, placement is skipped rather than treated as a sizing failure.

The exact placement method uses branch-and-bound. The scalable method uses:

- diversity-aware beam construction;
- one-replica relocation descent;
- pairwise swaps;
- small large-neighborhood repair; and
- randomized restarts.

The current objective combines:

- latency-weighted service communication between nodes; and
- a penalty for concentrating replicas of the same service on one node.

The current CLI does **not** accept per-service CPU/memory placement demand and therefore does not enforce node CPU or memory packing. The node model can carry allocatable values, and the library placement problem supports service resource requests, but the CLI constructs `NodePlacementProblem(cluster)` without them. In this version, `resources.yaml` should be understood primarily as a node communication/execution model.

When simulation is used, placement-specific communication latency and execution multipliers are attached to the simulator. Kubesizer recalibrates the fixed placement and runs final validation after placement. When analytical evaluation is used, non-unit node execution multipliers are rejected because the analytical path cannot certify them.

---

## 5. Portable YAML package: what the DevOps engineer supplies

## 5.1 Service declarations in portable `workload.yaml`

A portable workload begins with its canonical service declarations:

```yaml
services:
  api:
    namespace: production
    cpu_request_cores: 0.5
    cpu_limit_cores: 1.0
    memory_request_bytes: 536870912
    memory_limit_bytes: 1073741824
    concurrency_limit_per_replica: 32
```

### Field mapping

| YAML field | Unit | Meaning in the real system | Internal mapping | Affects |
|---|---|---|---|---|
| `services.<name>` | Identity | Canonical application service/workload | Service identity and joins | Topology, rates, sizing output |
| `namespace` | String | Kubernetes namespace containing the pods | Join key with measured pod rows | Evidence attribution only |
| `cpu_request_cores` | CPU cores | CPU reserved for one application replica | CPU entitlement used to turn CPU demand/request into an execution-time estimate | Characterized service demand and capacity reasoning |
| `cpu_limit_cores` | CPU cores | Cgroup CPU ceiling | Reported resource context; not assumed guaranteed capacity | Diagnostics |
| `memory_request_bytes` | Bytes | Reserved memory per replica | Resource context | Characterization/diagnostics; not current CLI placement packing |
| `memory_limit_bytes` | Bytes | Memory ceiling per replica | Resource context | Characterization/diagnostics; not current CLI placement packing |
| `concurrency_limit_per_replica` | Requests | Worker/thread/connection/admission slots per replica | Finite service admission resource | Simulation blocking/concurrency stability |

The CPU request is intentionally different from observed CPU use. Observed CPU use tells Kubesizer what was consumed; the request tells it what CPU share the scheduler reserves for the replica.

## 5.2 Request topology and SLOs in portable `workload.yaml`

A minimal request section in the same file is:

```yaml
request_classes:
  browse:
    concurrency_weight: 1.0
    entry_services:
      api: 1.0
    calls:
      api:
        database: 1.2
    call_execution: sequential
    end_to_end_latency_slos:
      mean_seconds: 0.25
```

Optional top-level sections are:

```yaml
load_forecast:
  multipliers: [1.0, 1.25, 1.5]

sizing_policy:
  maximum_capacity_utilization: 0.80

service_blocking_times:
  api:
    distribution: deterministic
    mean: 0.006

communication_latency:
  same_node_one_way: ...
  cross_node_one_way: ...
  unplaced_cross_node_probability: 0.6
```

### Request-class field mapping

| YAML field | Meaning | Internal mapping | Affects |
|---|---|---|---|
| `request_classes.<class>` | A bounded logical user/API operation | Request-class identity | Separate streams, latency arrays, SLO decisions |
| `concurrency_weight` | Expected fraction of external traffic belonging to this class; all class weights sum to 1 | Expected class mix checked against measured shares | Characterization consistency and traffic composition |
| `entry_services` | Probability that a class enters at each listed service; values for a class sum to 1 | Splits measured class arrival rate into service-specific external rates `gamma` | Traffic equations and simulation entry streams |
| `routing` | Conditional next-hop probabilities; unassigned probability exits | Routing matrix `P` | Analytical traffic equations and stochastic simulation routing |
| `visits` | Expected visits to each service per external class request | Analytical visit vector | Analytical service rates and mean latency only; not enough event order for simulation |
| `calls` | Expected child calls made by a source visit; may exceed 1 | Synchronous call graph / visit expansion | Simulation request tree, service load, communication latency |
| `call_execution` | `sequential`, `parallel`, or `uncertain` | Sibling-call timing semantics | End-to-end latency and held parent concurrency |
| `end_to_end_latency_slos.mean_seconds` | Maximum permitted request-class mean latency | Analytical or simulation constraint | Replica feasibility |
| `end_to_end_latency_slos.percentile` | Quantile and maximum latency; current quantile must be 0.95 | Simulation p95 constraint | Replica feasibility; forces simulation |
| `end_to_end_timeout_seconds` | Client-visible request deadline | Terminal timeout clock | Simulation completion/failure only |
| `maximum_terminal_failure_rate` | Maximum terminal failure fraction | Statistical rate constraint | Simulation feasibility; requires a timeout |
| `latency_boundary` | Where the end-to-end clock begins: `ingress` or `external_client` | Defines which latency components are legal | Prevents boundary double counting |
| `fixed_latency_overhead_seconds` | Deterministic latency added once per request | Legacy non-service request overhead | Mean and p95; not service capacity |
| `latency_overhead` | Distribution added once per request | Non-service request latency distribution | Simulated latency tail |
| `latency_components` | Named distributions such as client-to-ingress, ingress processing, or unmodeled residual | Independently sampled end-to-end components | Simulated end-to-end latency |

Exactly one flow representation—`routing`, `visits`, `workflow`, or `calls`—is accepted for each native request class. Portable workload parsing supports visits or calls; calls require an explicit execution mode.

### Top-level portable field mapping

| YAML field | Meaning | Internal mapping | Affects |
|---|---|---|---|
| `load_forecast.multipliers` | Workload factors to plan for | Largest multiplier scales measured class arrival windows | Stability floor and sizing workload envelope |
| `sizing_policy.maximum_capacity_utilization` | Maximum permitted use of bounded healthy request capacity; default `0.85` | Headroom multiplier applied to the per-replica capacity lower confidence bound | Confidence-bounded stability floor and minimum replica vector |
| `service_blocking_times.<service>` | Non-CPU time such as I/O/lock/external wait that retains an admission slot | Per-visit blocking distribution | Rich simulation concurrency and latency |
| `communication_latency.same_node_one_way` | One-way in-node/pod path latency distribution | Network delay for colocated service calls | End-to-end latency |
| `communication_latency.cross_node_one_way` | One-way cross-node delay distribution | Network delay for cross-node service calls | End-to-end latency and placement validation |
| `unplaced_cross_node_probability` | Estimated chance a call crosses nodes before placement is known | Mixture probability | Pre-placement simulation |

## 5.3 `measurements.yaml`

The manifest is a list of normalized evidence files:

```yaml
measurement_id: production-window-2026-08-31
methodology_version: prometheus-istio-kubernetes-v1
service_intervals: service-intervals.csv
service_class_intervals: service-class-intervals.csv
external_intervals: external-intervals.csv
service_edges: service-edges.csv
service_latency_histograms: service-latency-histograms.csv
external_latency_histograms: external-latency-histograms.csv
external_arrival_samples: external-arrival-samples.csv
capacity_observations: capacity-observations.csv
```

Paths are relative to the manifest. The version identifies the measurement semantics, not a YAML schema banner in the workload file.

### Measurement file mapping

| Manifest field | Required CSV identity/quantities | What Kubesizer derives | Used by |
|---|---|---|---|
| `service_intervals` | Fixed aligned `start,end,service,namespace,pod`, completed request count, CPU seconds | Service throughput, CPU demand/request, mean response, errors, throttling, memory, occupancy | Characterization, simulation service time, evidence gates |
| `service_class_intervals` | `start,end,service,request_class,request_count` | Class-specific service visits | Per-class topology and traffic propagation |
| `external_intervals` | `start,end,request_class,request_count`; optional errors and response-duration sum | External class arrival rate `gamma_c`, class share, mean boundary latency, error rate | Workload construction and SLO evidence |
| `service_edges` | `start,end,source_service,destination_service,call_count`; optional request class | Edge rate `C_ij`, routing probability where valid, visit/call ratio | Call graph, traffic propagation, communication cost |
| `service_latency_histograms` | `start,end,service,upper_bound_seconds,cumulative_count` | Aggregate service p50/p90/p95/p99 after summing buckets | Characterization/validation; not treated as exclusive CPU service time |
| `external_latency_histograms` | `start,end,request_class,upper_bound_seconds,cumulative_count` | Measured request-class p50/p90/p95/p99 | SLO evidence and validation diagnostics |
| `external_arrival_samples` | `timestamp,request_class` | Empirical interarrival samples/burstiness | Simulation arrival distribution and uncertainty |
| `capacity_observations` | Independent run ID, service, replicas, offered/completed rates, errors, `healthy`; optional throttling | One-sided lower healthy capacity bound per replica | Confidence-bounded stable floor |

### Important normalized columns

`service-intervals.csv` supports:

```text
start, end, service, namespace, pod,
request_count, cpu_seconds,
errors_4xx, errors_5xx,
throttled_seconds, memory_bytes,
mean_active_requests, max_active_requests,
response_duration_sum_seconds
```

The essential calculation is:

```text
CPU demand per request = CPU seconds consumed / completed requests
```

This is CPU-seconds/request, not literal wall-clock service time. Dividing by the reserved CPU cores provides an execution-time estimate under that CPU allocation. Inclusive Istio response duration remains separate because it can include downstream waits.

`capacity-observations.csv` supports:

```text
run_id, service, replicas, offered_rate_rps,
completed_rate_rps, error_rate, healthy,
cpu_throttling_ratio
```

`healthy=true` must mean more than “the process returned HTTP 200.” The load-test producer should predeclare gates showing that completed throughput tracks offered throughput, backlog is bounded, failures and scheduler delay are acceptable, the generator is not saturated, and the observation is sufficiently stationary. Kubesizer trusts this normalized evidence contract; it cannot reconstruct omitted load-generator state from one Boolean.

## 5.4 Where the raw data can come from

The portable schema is collector-independent, but its current methodology maps naturally to:

- Istio destination request counters for completed per-service requests;
- Istio source counters for service edges;
- Istio duration histograms for inclusive response latency;
- Kubernetes/cAdvisor container CPU and memory metrics;
- Kubernetes API resource requests and limits;
- a load generator for external arrivals, boundary latency, and independent capacity runs; and
- node-to-node probes for same-node and cross-node network delay.

Different clusters expose different Prometheus labels. A collector must discover and normalize those labels into the CSV identities above. In particular, `source_workload="unknown"` is external traffic only after restricting destination workloads to known entry points. Unknown-source health probes can contaminate that measurement and must be excluded or documented.

---

## 6. How the confidence-bounded stable floor is calculated

This stage exists only in the portable evidence path.

For each service, Kubesizer obtains:

- aligned service-arrival samples derived from measured class arrivals and visits; and
- independent healthy completed-throughput samples from capacity runs at fixed replica counts.

It calculates:

1. a one-sided upper Student-t bound on the service arrival rate;
2. a one-sided lower Student-t bound on per-replica healthy completed capacity;
3. Bonferroni allocation over all service-arrival and tested-capacity bounds;
4. a usable capacity equal to the bounded per-replica rate multiplied by `sizing_policy.maximum_capacity_utilization` (default 0.85); and
5. the smallest candidate satisfying:

```text
replicas * usable_capacity_lower_bound > arrival_rate_upper_bound
```

The chosen floor is never below the analytical queue-stability vector.

Default evidence requirements include at least three complete aligned workload windows and at least three independent healthy capacity runs at a usable replica count. Missing evidence causes the portable flow to stop rather than silently substitute analytical stability as deployment proof.

If a selected replica count was not directly capacity-tested, Kubesizer uses the worst per-replica lower capacity bound observed at tested counts and emits an explicit scaling warning. Therefore the claim is:

> Confidence-bounded stable for the measured/forecast workload, assuming future conditions remain comparable and per-replica capacity at the selected count is no worse than the worst bounded tested value.

It is **not**:

> Stable for every possible distribution, traffic burst, dependency failure, or cluster condition.

---

## 7. Native self-contained workload YAML

The direct path is useful when the user already has distribution models. A minimal file is:

```yaml
request_classes:
  request:
    concurrency_weight: 1.0
    end_to_end_latency_slos:
      mean_seconds: 1.0
    routing: {}

traffic_model:
  entry_services:
    request:
      service:
        distribution: exponential
        rate: 1.0
  service_times:
    service:
      distribution: exponential
      rate: 2.0
```

Field names carry their units: rates are per second and durations are seconds. The removed generic `time_unit` and `schema_version` keys are not part of the current workload schema.

## 7.1 Native traffic fields

| Field | Meaning | Internal use |
|---|---|---|
| `traffic_model.entry_services.<class>.<service>` | External interarrival process for that class and entry service | Generates concurrent external streams and defines class arrival rates |
| `traffic_model.service_times.<service>` | Per-visit execution/service-time distribution | Service rate, variability, and C++ execution samples |
| Class-specific service-time mapping, where supplied | Different demand for each request class at one service | Per-class service mixture and simulation samples |
| `traffic_model.communication_latency` | Same/cross-node one-way distributions | Network delay before and after placement |
| `traffic_model.input_uncertainty` | Aligned arrival/service multiplier epochs | Moving-block bootstrap retaining correlated regimes |

`concurrency_weight` is required for every class and all weights sum to one. In the native path it should agree with the total rates declared under `entry_services`; it is the intended external traffic share, not an extra arrival-rate multiplier.

## 7.2 Supported distributions

The native parser and C++ simulator support:

| Quantity | Supported forms |
|---|---|
| Interarrival times | Exponential, deterministic, gamma, lognormal, Weibull, empirical interarrival samples, block-bootstrap arrival trace, custom C++ plugin |
| Service/blocking/network durations | Exponential, deterministic, gamma, lognormal, Weibull, empirical samples, custom C++ plugin |

Typical parameterizations are:

```yaml
# Arrival process: mean interarrival is 1 / rate
distribution: gamma
rate: 20.0
scv: 2.0

# Duration process
distribution: lognormal
mean: 0.015
scv: 1.5
```

Empirical samples may be supplied inline or from CSV and sampled by IID replacement, Bayesian bootstrap, or cluster bootstrap. Arrival traces use timestamps and block bootstrap to preserve local burst structure.

Custom distributions are native shared-library plugins using the ABI in `native_backend/cpp/include/kubesizer/custom_distribution.h`. The workload supplies the library, symbol, ABI version, declared mean/SCV, and numeric parameters. Kubesizer verifies the library hash between parsing and execution. Declared moments are trusted metadata and should be independently tested against samples from the plugin.

## 7.3 Execution and queue semantics

Per service, the native file can declare:

| Field | Values | Meaning |
|---|---|---|
| `execution_model` | `fixed_slots`, `processor_sharing` | Either discrete service slots or CPU sharing among active visits |
| `processing_slots_per_replica` | Positive integer | CPU work capacity per replica |
| `concurrency_limit_per_replica` | Positive integer | Maximum admitted visits retaining a worker/connection slot |
| `replica_queue_model` | `pooled`, `independent_random`, `independent_round_robin` | One pooled queue or Kubernetes-like separate replica queues |
| `non_cpu_blocking_time` | Duration distribution | I/O/wait after CPU work that releases CPU but retains admission |

These fields are why a rich workload may require simulation even if its SLO is a mean. The strict analytical queue model does not certify every held-resource interaction.

---

## 8. Latency inputs and what they map to

Kubesizer separates latency into categories so that the same delay is not counted twice.

| Real-world quantity | Preferred input | Internal model | Does it reduce service capacity? |
|---|---|---|---|
| CPU execution | CPU seconds/request plus CPU request, or native service-time distribution | Service work at a queue/server | Yes |
| Non-CPU wait while a worker is held | `service_blocking_times` or `non_cpu_blocking_time` | Post-CPU blocking while admission remains occupied | Through concurrency, not CPU |
| Downstream call duration | `calls` plus child service and network models | Synchronous child request tree | Indirectly through held parent concurrency |
| Same/cross-node communication | `communication_latency` or `resources.yaml` matrix | One-way delay on service edges | No direct CPU capacity reduction |
| Client-to-ingress | `latency_components.client_to_ingress` | Added once at external-client boundary | No |
| Ingress processing | `latency_components.ingress_processing` | Added once at ingress boundary | No |
| Remaining unmodeled delay | `latency_components.unmodeled_residual` or legacy overhead | Added once per end-to-end request | No |

## 8.1 The residual latency term

The residual is acceptable when it is explicitly characterized and kept separate from service demand. A low-load characterization can estimate:

```text
residual = observed boundary latency
         - modeled CPU execution
         - modeled downstream work
         - modeled communication components
```

It may contain ingress/proxy processing, serialization, scheduler delay, TLS, client path delay, or other effects not represented elsewhere. The characterization and validation data must be separate: estimate/freeze the residual on one run, then test predictions on fresh runs.

The residual is environment and boundary specific. It is not a universal constant that can be copied from STELAR or another cluster. A mean residual is broadly extractable from aligned counters at low load. A trustworthy residual distribution requires paired per-request evidence or another defensible sampling method; subtracting unrelated p95 values is invalid.

---

## 9. `resources.yaml` and placement mapping

A current resources file looks like:

```yaml
nodes:
  - name: node-a
    execution_time_multiplier: 1.0
    communication_latency_seconds:
      node-b: 0.0012

  - name: node-b
    execution_time_multiplier: 1.0
    communication_latency_seconds:
      node-a: 0.0014
```

| Field | Meaning | Internal mapping | Effect |
|---|---|---|---|
| `nodes[].name` | Placement target identity | Node name | Output placement and communication lookup |
| `execution_time_multiplier` | Relative execution slowdown/speedup on this node | Multiplies simulated service execution | Placement-aware simulation; must be 1.0 in analytical CLI mode |
| `communication_latency_seconds.<other>` | Directed one-way delay to another node | Node latency matrix | Placement objective and post-placement simulation |

The matrix may be asymmetric. Missing self-edges imply local behavior handled by the cluster model.

Again, CPU/memory packing is not part of the current CLI placement flow because there is no per-service placement-demand input in the CLI call. Do not interpret the placement result as a Kubernetes schedulability proof.

---

## 10. Output and failure behavior

Successful human output is intentionally small:

```yaml
services:
  api:
    recommended_replicas: 3
    node_placement:
      node-a: 2
      node-b: 1
  database:
    recommended_replicas: 2
    node_placement: null
```

When placement is skipped, `node_placement` is `null`. The command creates no result files unless `--out` is supplied.

Kubesizer does not emit a recommendation when a required proof step fails. Examples include:

- portable evidence cannot establish a confidence-bounded capacity floor;
- no feasible replica vector exists below the configured maximum;
- simulation remains inconclusive at the maximum replication count;
- eligible requests cannot be drained;
- p95 bootstrap resolution or eligible sample counts are insufficient;
- final fresh-seed validation fails;
- placement changes the simulated latency enough to invalidate the SLO; or
- analytical mode is requested for unsupported semantics.

This “fail instead of fabricate” behavior is deliberate. It means the requested evidence level was not established; it does not necessarily prove that the real deployment cannot meet the SLO.

---

## 11. Recommended operating recipes

## 11.1 Strongest current deployment-informed recommendation

Use when the DevOps engineer can provide aligned observation windows and independent capacity runs.

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation \
  --replica-search auto \
  --skip-placement
```

Interpretation:

- The search never goes below a joint-confidence capacity floor.
- The SLO is decided using a calibrated stochastic model and familywise confidence intervals.
- The selected vector is tested again with fresh seeds.
- The conclusion is conditional on measurement quality and model representativeness.

## 11.2 Fast conservative screening

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation analytical \
  --replica-search auto \
  --skip-placement
```

Interpretation:

- The empirical capacity floor still prevents removal below bounded healthy capacity.
- Mean latency uses an analytical queue approximation.
- This is not p95 certification and is rejected if rich resource-holding semantics are declared.

## 11.3 Large service topology

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation \
  --replica-search scalable \
  --resources resources.yaml \
  --placement-search scalable
```

Interpretation:

- Feasibility remains certified by the evaluator.
- Search optimality is not guaranteed.
- Report the heuristic method, budgets, and local certificate if exposing detailed library results.

## 11.4 Small synthetic benchmark

```bash
kubesizer \
  --workload workload.minimum.yaml \
  --replica-evaluation analytical \
  --replica-search exact \
  --skip-placement
```

Interpretation:

- Useful for checking model/search behavior.
- Does not contain real deployment evidence.

---

## 12. What each data source can and cannot establish

| Data supplied | What it supports | What it does not establish by itself |
|---|---|---|
| External class counters | Arrival rates and class shares | Burst tails without samples/trace |
| Service destination counters | Observed service throughput/visits | Causal next-hop ordering |
| Service edge counters | Call rates and communication weights | Sequential versus parallel sibling calls |
| CPU counters plus completed requests | CPU demand/request | Wall-clock service time or I/O wait |
| CPU request | Reserved CPU entitlement | Actual saturation throughput |
| Istio response histogram | Inclusive response percentiles | Exclusive application CPU demand |
| External latency histogram | Observed boundary p95 | Per-request decomposition into components |
| Independent capacity runs | Healthy throughput envelope | Unlimited linear scaling to untested counts |
| Concurrency-limit configuration | Worker/admission capacity | Actual queue discipline if the application differs |
| Node probe matrix | Communication delay by placement | Shared-network congestion under all loads |
| Low-load residual characterization | Baseline hidden latency for that environment | A portable universal overhead |

The portable package is designed around data a DevOps engineer can usually extract without modifying application source code. It intentionally keeps measured quantities, inferred quantities, and assumptions distinct.

---

## 13. Honest guarantees and limitations

### What the current flow can state

- A portable recommendation does not go below the empirical confidence-bounded stability floor.
- A simulation-feasible configuration has request-class confidence bounds on the declared mean or p95 under the calibrated model.
- Final simulation validation uses seeds not used by optimization.
- Exact search can prove the cheapest configuration only within its bounds, evaluator, and resolved statistical states.
- Scalable search returns a certified-feasible incumbent when it succeeds, but not a global guarantee.
- A placement result has the declared deterministic placement objective and is rechecked in simulation mode.

### What it cannot state

- Stability under every traffic distribution or future workload.
- Accuracy when the measurement windows are not representative.
- Protection against unmeasured databases, caches, external APIs, retries, autoscaling, or cluster contention.
- Kubernetes schedulability from the current CLI placement result.
- A universal residual latency across deployments.
- Global optimality for heuristic search.
- Physical optimality merely because bounded model search is exact.

The practical interpretation is “defensible initial sizing for a declared measured workload,” not “permanent capacity guarantee.” Rerun the portable evidence flow idempotently when workload, topology, service behavior, resources, or the latency boundary changes.

---

## 14. Code map

| Responsibility | Current implementation |
|---|---|
| CLI orchestration | `kubesizer_cli.py`: `_load_workflow_system`, `_run_replica_workflow`, `_command_run` |
| Portable YAML parsing | `characterization/parser.py` |
| Normalized measurement semantics | `characterization/methodology.py`: `measurement_methodology_contract` |
| Characterization | `characterization/analysis.py` |
| Portable-to-simulator conversion | `characterization/simulation.py` |
| Confidence-bounded stability floor | `characterization/stability.py`: `estimate_statistical_stability_floor` |
| Native workload parsing | `performance_models/workload.py`: `parse_workload_spec` |
| Traffic and analytical model | `performance_models/system.py`, `performance_models/analytical.py` |
| C++ simulator adapter | `native_backend/backend.py`: `CppSimulationBackend` |
| C++ event kernel | `native_backend/cpp/src/main.cpp` |
| Warm-up calibration | `calibration/workflow.py` |
| Measurement-duration calibration | `calibration/measurement.py` |
| Statistical profile and seeds | `calibration/profile.py`, `calibration/seeds.py` |
| Per-configuration simulation classification | `optimization/simulation_evaluation.py` |
| Final validation | `optimization/replica_validation.py` |
| Exact replica search | `optimization/uniform_cost.py` |
| Scalable replica search | `optimization/scalable_replica.py` |
| Exact placement | `optimization/branch_and_bound_node_placement.py` |
| Scalable placement | `optimization/scalable_node_placement.py` |
| Search-mode selection | `optimization/selection.py`, `optimization/mode.py` |
| End-to-end public workflow | `statistical_workflow.py` |
| Runtime estimate/progress | `runtime_estimation.py`, `kubesizer_cli.py:_ConciseProgressReporter` |

---

## 15. One-page decision guide

1. **Do you have real aligned deployment measurements and fixed-replica capacity runs?**  
   Yes: use the portable evidence path.  
   No: use the direct model path, but label the result model-based.

2. **Is the SLO p95, or are timeouts/rich concurrency important?**  
   Yes: use `--replica-evaluation simulation`.  
   No, mean only and compatible assumptions: analytical is available.

3. **Is the bounded replica space small?**  
   Use `auto`; it selects exact through 10,000 configurations. Force scalable when runtime matters more than proof of bounded optimal cost.

4. **Do you need node placement?**  
   Supply `--resources`. Otherwise omit it or use `--skip-placement`.

5. **Do you need CPU/memory schedulability?**  
   The current CLI does not provide it. Validate with the Kubernetes scheduler/resource policy separately or extend the placement input/API.

6. **What should be reported to a user?**  
   Report the recommended replica vector, optional placement, evaluator, search method, workload envelope, evidence date, and the conditional nature of the stability/SLO statement.

---

## 16. Final recommended wording for a Kubesizer result

For a successful portable simulation run:

> Kubesizer recommends the reported initial replica vector for the declared workload and forecast envelope. The search was constrained not to go below a 95% joint-confidence capacity floor derived from aligned arrival windows and independent healthy capacity runs. The selected configuration satisfied the declared request-class latency SLOs in calibrated C++ simulation and passed a fresh-seed final validation. The conclusion remains conditional on measurement representativeness, declared call/concurrency semantics, the residual/network latency characterization, and the assumption that selected replica counts do not have worse per-replica capacity than the conservative tested bound.

For a direct-model run:

> Kubesizer recommends the reported replica vector under the distributions, topology, service semantics, and SLOs declared in the workload file. The lower bound is analytical queue stability. Unless deployment capacity evidence was supplied through the portable workflow, this is a model-based initial recommendation rather than a confidence-bounded statement about a live deployment.
