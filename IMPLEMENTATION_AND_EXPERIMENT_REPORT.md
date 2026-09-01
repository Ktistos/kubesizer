# Independent Implementation and Experiment Audit

**Repository audited:** `/home/ktistos/work/Kubesizer`  
**Original audit date:** 29 August 2026

**Enriched edition:** 30 August 2026

**Audited branch and revision:** `feature/statistical-simulation-workflow`, base commit `6d223c154df687100e9d7023e19d674e3cc5fa17`, plus the current uncommitted audit-correction working tree

**Audit method:** direct source inspection, test execution, native compilation, retained-artifact analysis, a current-semantics replay, and an earlier read-only inspection of the completed remote experiment
**Purpose:** an evidence-based thesis-status briefing, not a project-description document

## Audit rules and status vocabulary

This report was reconstructed from the current executable paths and retained data. Older README files, previous reports, and comments were used only as leads. A claim is included as fact only when it is supported by current code, a passing check, or a retained experimental artifact.

The following terms have precise meanings throughout this report:

- **Implemented and verified:** current code exists and is exercised by a passing deterministic or integration test in this audit.
- **Implemented but not experimentally validated:** code and tests exist, but no retained real or controlled-system experiment demonstrates prediction or decision quality.
- **Partially implemented:** the mechanism exists, but an important contract is missing or not enforced by the active path.
- **Planned but not implemented:** no active implementation was found.
- **Experimentally measured:** raw or processed observations are retained with enough context to reconstruct the result.
- **Inferred but not directly measured:** a value is calculated from other measurements or assigned through a modelling assumption.

Experiment-table statuses use exactly the requested vocabulary: **VERIFIED RESULT**, **PRELIMINARY RESULT**, **INCOMPLETE**, **IMPLEMENTED BUT NOT RUN**, or **UNCLEAR**. A verified result may be negative. “Verified” means the reported observation is supported by retained evidence; it does not mean the system under test succeeded.

## Executive audit conclusion

Kubesizer is currently a substantial research prototype with four main layers:

1. a native or telemetry-derived workload model;
2. analytical queueing approximations used for traffic propagation, stability, mean latency, and candidate ranking;
3. a mandatory C++20 discrete-event simulator with a Python statistical-control layer;
4. exact and scalable heuristic optimizers for replica sizing and node placement.

The implementation is broad and its deterministic test coverage is strong: the full current suite passed **556 tests**, and **74 focused native/simulation tests** also passed with AddressSanitizer and UndefinedBehaviorSanitizer. The C++ backend compiled in both Release and sanitizer modes.

The experimental conclusion is much narrower. The C++ implementation decision is well supported: on the frozen calibration-like benchmark it was **35.65× faster than custom Python** with a bootstrap 95% interval of **[34.82×, 36.20×]**, and a direct long-horizon run measured **31.8×** speedup. This validates the performance motivation, not the current rich simulator's predictive accuracy.

Replica-sizing quality is **not yet established for general deployments**. The largest evaluated deployment-family study completed 405 real-cluster runs. Twelve of its 20 workload-family/load-point decisions were rejected by evidence-quality gates. Of the eight accepted decisions, six matched the smallest passing configuration in the tested set and two were underprovisioned. At the four 30-replication target-validation points, the recommendation passed for the repeated-visit and shared-multiclass families but failed for the single-service and fan-out families. Therefore the evidence supports “promising but not generally reliable,” not a universal recommendation claim. The original VM data were later lost; only selected replay inputs and a partial cluster-side recovery remain.

The audit found correctness gaps, but the current tree now enforces several direct contracts: global warm-up validation uses the post-warm-up stability result; analytical mode rejects unsupported semantics; timeout feasibility includes a statistically controlled failure-rate bound; and placement-aware final checks use the selected placement. CLI placement is intentionally topology-only in this version: it does not consume pod CPU/memory requests or claim Kubernetes resource feasibility. These corrections are code-tested; they are not yet externally validated on a fresh deployment campaign.

No retained experiment establishes the quality of the current exact-versus-scalable replica search, the current placement objective, percentile-SLO sizing, or the full current calibration-to-optimization-to-final-validation workflow on a real deployment. Where those claims would otherwise be tempting, this report states **NOT ESTABLISHED FROM CURRENT EVIDENCE**.

## Decision-grade snapshot

This table separates what the current prototype can do from what the evidence allows the thesis to claim.

| Question | Current answer | Evidence boundary |
|---|---|---|
| Is the C++ simulator implemented and mechanically tested? | **Yes.** | Release and sanitizer builds pass; deterministic lifecycle, state, distribution, call, timeout, and measurement-cohort tests pass. |
| Can the software statistically classify a candidate under its own model? | **Yes.** | Calibration, independent replications, multiplicity-adjusted intervals, sequential decisions, and fresh final validation are implemented and test-covered. |
| Does a simulator confidence interval prove that the deployed Kubernetes system will pass? | **No.** | It quantifies simulation randomness conditional on the workload model. It does not cover wrong service-demand, blocking-pool, routing, residual-latency, or deployment assumptions. |
| Is analytical stability sufficient to claim an SLO-safe deployment? | **No.** | It is a necessary model-level load condition. Retained STELAR evidence shows accurate flow and stable analytical equations can coexist with severe live latency and failures. |
| Is the current replica recommendation generally safe for arbitrary deployments? | **Not established.** | The largest family study contained unsafe decisions; the safest current retained-data model is conservative on the stronger target subset but has not been freshly deployed. |
| What did the latest complete in-memory workflow do? | It calibrated `W=87.918 s`, selected `M=1536 s`, selected five replicas, and passed 30-seed simulated final validation at `90.006 ms [89.376, 90.666]` under a 100 ms mean SLO. | This proves that the current workflow executes coherently on one retained characterized model. Five replicas were never tested on the live deployment, so deployment feasibility is unverified. |
| What does node placement mean in the CLI? | **Topology-only guidance.** | It optimizes modelled communication and spreading. It does not use pod requests/limits or certify Kubernetes schedulability. |
| What is the strongest defensible thesis position today? | Kubesizer is a statistically cautious capacity-planning prototype and experimental platform, not yet a universally trustworthy automatic sizer. | Strong kernel-performance and implementation evidence; mixed and incomplete external predictive evidence. |

# 1. Current system reconstruction

## 1.1 Active public entry points

The active user entry point is the single-command CLI in `kubesizer_cli.py`:

- `build_parser()` (`kubesizer_cli.py:905`) defines workload, deployment evidence, replica-evaluation, replica-search, placement-search, bounds, worker, progress, and output options.
- `_load_workflow_system()` (`kubesizer_cli.py:110`) chooses either a native workload YAML or the portable workload-plus-measurement evidence path; portable service facts live under `workload.services`.
- `_run_replica_workflow()` (`kubesizer_cli.py:683`) selects analytical or calibrated-simulation sizing.
- `_command_run()` (`kubesizer_cli.py:745`) runs sizing, optional placement, and—when simulation is used—fresh final validation after placement effects have been installed.

The equivalent library interface is in `statistical_workflow.py`:

- `calibrate()` (`statistical_workflow.py:523`);
- `evaluate_configuration()` (`statistical_workflow.py:561`);
- `optimize_with_calibrated_simulation()` (`statistical_workflow.py:584`);
- `optimize_replica_sizing()` (`statistical_workflow.py:824`);
- `validate_final_configuration()` (`statistical_workflow.py:912`).

The CLI prints concise progress and final per-service recommendations. It writes a result file only when `--out` is supplied. Calibration profile serialization/reporting functions exist as library utilities, but the active CLI recalibrates in memory rather than loading a prior profile.

## 1.2 Inputs

### Native workload input

The native schema is parsed by `parse_workload_spec()` in `performance_models/workload.py:1200`. Its immutable representations are `DistributionSpec`, `TrafficModelSpec`, and `ParsedWorkload` (`performance_models/workload.py:17`, `:55`, and `:100`). The root `workload.yaml` currently declares:

- three request classes: `read_heavy`, `write_heavy`, and `compute_heavy`;
- a mandatory concurrency weight for each class, summing to one;
- a p95 end-to-end latency SLO of one second per class;
- per-class probabilistic routing;
- one external entry service (`api`) per class;
- exponential external interarrivals and exponential service times.

The parser also supports richer inputs than the root example:

- external interarrivals: exponential, deterministic, gamma, lognormal, Weibull, empirical samples, empirical clusters, block-bootstrap arrival traces, and custom C++ distribution plugins;
- service or overhead durations: exponential, deterministic, gamma, lognormal, Weibull, empirical samples/clusters, and custom plugins;
- request-class flow descriptions: probabilistic routing, ordered workflows, explicit visit ratios, and synchronous call graphs with sequential or parallel child calls;
- class-specific service-time distributions;
- per-replica processing slots, concurrency limits, processor-sharing execution, pooled or independent replica queues;
- fixed or distributed latency components, end-to-end timeouts, and failure-rate SLOs;
- same-node/cross-node communication distributions and joint input uncertainty.

Important restriction: every declared percentile SLO must be p95, and all request classes in one workload must use the same latency-SLO kind (`performance_models/workload.py:895` and `:1170`). Explicit visit ratios support analytical mean-flow calculations but do not define an executable path; simulation rejects such a class unless an executable workflow/call graph is available (`System.require_simulation_compatible_workload()`, `performance_models/system.py:1190`).

### Portable DevOps evidence input

The alternative input path consumes three files: a deployment description, a request-class/workload description, and a measurement manifest. Parsing begins in `characterization/parser.py:1160` and file loading in `:1190`. Immutable structures are defined in `characterization/models.py`.

The evidence schema can represent:

- service identities, namespace, Kubernetes CPU request, optional CPU limit, memory values, and optional concurrency limits;
- request classes, entry services, mean visit ratios or explicit call graphs, SLOs, and latency-boundary components;
- time-window service arrivals/completions, application-container CPU use, errors, throttling, memory, active requests, and duration;
- class-attributed external intervals and arrival samples;
- service-edge call rates and latency histograms;
- independent capacity observations at known replica counts.

The CPU request is the model's capacity basis (`DeploymentService.cpu_capacity_cores`, `characterization/models.py:26`). A CPU limit is treated as a reported ceiling, not guaranteed scheduler capacity.

`characterization.analysis.characterize_deployment()` derives visit ratios, CPU demand, flow balance, error rates, aligned uncertainty windows, and capacity envelopes. `recommend_initial_replicas()` (`characterization/sizing.py:85`) creates a conservative **construction seed**, not a final optimum. Its two available bounds are:

- CPU bound: the target arrival rate times a p90 window-level CPU demand, multiplied by 1.10, divided by CPU-request capacity at target utilization 0.65;
- demonstrated-capacity bound: a one-sided Student-t lower mean throughput bound, used at 85% utilization and only at an empirically observed replica count.

The result is the maximum of the one-replica floor and available bounds. It deliberately does not linearly extrapolate a capacity point to unobserved replica counts (`characterization/sizing.py:85-224`). `assess_initial_sizing_risk()` in `characterization/readiness.py` gates missing class arrival data, missing SLO statistics, flow errors, failures, throttling, load-dependent demand, and extrapolation. The simulator builder in `characterization/simulation.py` converts CPU-seconds/request to reserved-capacity execution time and can construct call-order sensitivity variants.

This portable seed does not become a hard lower bound. In scalable optimization it is a construction start; deletion may move down to the analytical stability vector. Exact search always begins at the analytical lower bound.

### Cluster resources

`resources.yaml` is parsed by `Cluster` and `Node` (`performance_models/cluster.py`, `performance_models/node.py`). The active sample defines node names, directed communication latency, and execution-time multipliers. Allocatable CPU/memory metadata is optional.

The placement library can enforce per-service CPU and memory requests when a library caller supplies them (`NodePlacementProblem.__init__`, `optimization/node_placement.py`). The active CLI deliberately constructs `NodePlacementProblem(cluster)` without such requests. Therefore normal CLI placement preserves replica totals and optimizes communication/spreading, but it neither reads pod requests/limits nor certifies that Kubernetes node resources can host the recommendation.

## 1.3 Component status table

| Component | Active implementation | Audit classification | Main evidence |
|---|---|---|---|
| Workload/request-class parser | `performance_models/workload.py` | Implemented and verified | Parser/distribution/schema tests pass |
| Portable evidence parser and characterization | `characterization/` | Implemented and verified at unit/integration level | Characterization, evidence I/O, readiness tests pass |
| Traffic equations and stability | `performance_models/system.py` | Implemented and verified | Analytical and consistency tests pass |
| M/M/c and M/G/1 means | `performance_models/analytical.py` | Implemented and verified | Formula tests pass |
| M/G/c, G/M/c, G/G/c means | Variability-adjusted Erlang-C approximation | Implemented, experimentally weak | Tests verify calculation; STELAR evidence shows failure near capacity |
| Analytical end-to-end mean | `System.get_request_class_mean_latencies()` | Implemented, partly experimentally evaluated | Several retained mean-latency studies; mixed/negative results |
| C++ simulator | `native_backend/cpp/src/main.cpp` | Implemented and verified semantically by tests | Release/sanitizer build; deterministic tests pass |
| Rich simulator predictive accuracy | Current semantics version v13 | Implemented and replayed against retained evidence; mixed result | Current target replay gives 75% decision agreement for the safest portable model |
| Statistical calibration | `calibration/` | Implemented and exercised end to end in memory | Unit tests and retained-model replay pass; no clean live current-revision profile artifact |
| Cluster-bootstrap SLO evaluation | `metrics/cluster_bootstrap.py`, `optimization/simulation_evaluation.py` | Implemented and verified at test level | Focused tests pass |
| Exact replica sizing | Uniform-cost search | Implemented and verified on small tests | Unit comparisons; no retained application-scale quality experiment |
| Scalable replica sizing | randomized greedy + deletion + VNS | Implemented and verified on synthetic tests | No retained benchmark-quality study |
| Exact placement | branch-and-bound | Implemented and verified on tests | No retained real placement experiment |
| Scalable placement | beam + relocation/swap + LNS | Implemented and verified on tests | No retained objective/quality experiment |
| CLI topology-only placement | Network/spreading placement without pod resource requests | Implemented and verified | Direct active-path inspection and CLI tests |
| Runtime estimation | `runtime_estimation.py` | Implemented and tested | No retained estimator-accuracy experiment |
| Calibration/evaluation plots | `calibration/reporting.py`, `statistical_reporting.py` | Implemented but not active by default | Plot/report tests; no current retained plots in this repo |
| Persistent evaluation cache | None in active workflow | Planned/not implemented | Only in-process per-problem caches exist |

# 2. Mathematical methodology actually implemented

## 2.1 Notation

For service (i), request class (k), and replica vector (c):

| Symbol | Implemented meaning |
|---|---|
| \(\gamma_{ki}\) | external arrival rate of class \(k\) directly entering service \(i\) |
| \(P_{k,ij}\) | probability that a routed class-\(k\) visit leaves \(i\) for \(j\) |
| \(F_{k,ij}\) | mean number of synchronous child calls from a visit at \(i\) to \(j\) |
| \(v_{ki}\) | explicitly supplied expected visits to service \(i\) per external class-\(k\) request |
| \(\lambda_{ki}\) | effective class-\(k\) visit rate at service \(i\) |
| \(\lambda_i=\sum_k\lambda_{ki}\) | aggregate service arrival/visit rate |
| \(E[S_{ki}]\), \(C^2_{s,ki}\) | class-specific mean and squared coefficient of variation of service work |
| \(p_i\) | processing slots contributed by one replica |
| \(c_i\) | replica count |
| \(m_i=c_ip_i\) | analytical processing-server count |
| \(\rho_i\) | utilization of processing capacity |
| \(T_i\) | analytical mean service residence time, including queue wait |
| \(S_k\) | request-class latency SLO |

## 2.2 Request-class traffic and concurrency weights

The structured workload includes an external-rate distribution for every class/entry stream and a concurrency weight. In `System.__init__()` (`performance_models/system.py:91-373`), the configured total rate across all classes is preserved, but each class's total is rescaled to the declared weight while its within-class split across entry services is preserved:

\[
\gamma_k^{\text{effective}}
=
\left(\sum_h\gamma_h^{\text{configured}}\right)w_k.
\]

Thus classes execute concurrently as independent external streams; a weight is not a thread count or closed-system population.

For probabilistic routing, the implementation solves:

\[
\lambda_k=(I-P_k^\mathsf{T})^{-1}\gamma_k.
\]

This is implemented in `System.__calculate_effective_arrival_rates()` (`performance_models/system.py:773`). Exit probability is the unassigned remainder of a row, added as an edge with `target=None` in `Service.register_edges()` (`performance_models/service.py:34-55`). Routing loops are supported if the open-network solution exists.

For a synchronous call graph, the same linear equation is used with the mean offspring/call matrix (F_k):

\[
\lambda_k=(I-F_k^\mathsf{T})^{-1}\gamma_k,
\qquad \rho(F_k)<1.
\]

The spectral-radius check and solve are in `System.__calculate_request_class_arrival_rates()` (`performance_models/system.py:788-809`). This is a mean visit-rate calculation. It does not by itself model blocking order or parallel critical paths; those are simulator semantics.

For an explicit visit-ratio class, the implementation does not solve a transition matrix:

\[
\lambda_{ki}=\gamma_kv_{ki}.
\]

This supports observed fan-out or repeated visits whose outgoing ratios need not sum to one, but is analytical only unless an executable call order is also supplied.

## 2.3 Aggregate service-time moments

When request classes have different service demands at the same FCFS service, the implementation calculates an arrival-weighted mixture (`System.__calculate_service_time_moments()`, `performance_models/system.py:839-885`):

\[
E[S_i]=\frac{\sum_k\lambda_{ki}E[S_{ki}]}{\lambda_i},
\]

\[
E[S_i^2]=
\frac{\sum_k\lambda_{ki}(1+C^2_{s,ki})E[S_{ki}]^2}{\lambda_i},
\qquad
C^2_{s,i}=\frac{E[S_i^2]}{E[S_i]^2}-1.
\]

This includes both within-class variability and between-class differences in means. It is appropriate for the aggregate queue approximation, but class-specific analytical latency then applies the same aggregate queue wait to all classes at that service.

## 2.4 Arrival variability

`System.__calculate_effective_arrival_scvs()` (`performance_models/system.py:612`) iteratively approximates variability propagation. Independent stream superposition uses:

\[
C_a^2 \approx 1+\sum_j(\lambda_j/\lambda)^2(C_{a,j}^2-1),
\]

implemented in `combine_independent_stream_scvs()` (`performance_models/analytical.py:145`). Routing thinning and departure-SCV approximations are then iterated until convergence. These are approximations and assume stationary renewal-like inputs and independence that may not hold for correlated microservice traffic.

## 2.5 Utilization and stability

The aggregate mean service rate is \(\mu_i=1/E[S_i]\), with \(m_i=c_ip_i\) processing servers. `System.solve()` (`performance_models/system.py:887`) calculates:

\[
\rho_i=\frac{\lambda_i}{m_i\mu_i}
=\frac{\sum_k\lambda_{ki}E[S_{ki}]}{c_ip_i}.
\]

Strict analytical stability requires \(\rho_i<1\) for every service. The componentwise minimum stable replicas are calculated by `System.get_minimum_stable_replicas()` (`performance_models/system.py:1507`):

\[
c_{i,\min}
=
\left\lfloor
\frac{\sum_k\lambda_{ki}E[S_{ki}]}{p_i}
\right\rfloor+1.
\]

This vector is a necessary lower bound **under the implemented stationary mean-work model**. It is not proof of real-cluster stability when the supplied demand omits an actual bottleneck, demand changes with load, the execution model is wrong, or traffic is nonstationary.

## 2.6 Queueing models and mean response

`select_queueing_model()` in `performance_models/analytical.py:22` selects one model per service:

- Poisson arrivals + exponential service: exact stationary M/M/c Erlang-C mean;
- Poisson arrivals + general service + one server: exact M/G/1 Pollaczek-Khinchine mean;
- M/G/c, G/M/c, and G/G/c: an approximation.

For M/M/c, the implementation computes exact Erlang-C waiting probability and:

\[
E[T_i]=\frac1{\mu_i}+
\frac{P(\text{wait})}{m_i\mu_i-\lambda_i}.
\]

For M/G/1:

\[
E[T_i]=E[S_i]+
\frac{\lambda_iE[S_i^2]}{2(1-\rho_i)}.
\]

For the other cases, the corresponding M/M/c queue wait is multiplied by:

\[
\frac{C_{a,i}^2+C_{s,i}^2}{2}.
\]

The exact implementations are `mmc_mean_response_time()` and `mg1_mean_response_time()`; the approximation is `variability_adjusted_mean_response_time()` (`performance_models/analytical.py:72-142`). The approximation is not a general exact G/G/c solution.

## 2.7 Analytical end-to-end mean latency

`System.get_request_class_mean_latencies()` (`performance_models/system.py:1648`) calculates the aggregate queue wait per service as analytical response minus aggregate mean service. It then estimates class \(k\)'s latency as:

\[
\widehat L_k
=
\sum_i v_{ki}\left(E[S_{ki}]+E[W_i]\right)
+E[D_{\text{network},k}]
+E[D_{\text{overhead},k}].
\]

The network term is the expected call/transition count multiplied by expected node latency (`performance_models/system.py:1546-1646`). With a placement, source and destination replicas are assumed uniformly selected according to their fractions on nodes. Synchronous call graphs use a round trip. Without placement, same-node and cross-node means are mixed by a configured probability.

The overhead term is a measured or declared mean. The code supports decomposed components such as client-to-ingress and ingress processing, and it prevents obviously overlapping component combinations in the schema (`performance_models/workload.py:780-980`). For simulation, full component distributions are sampled. For the analytical model, only their means are used.

This calculation predicts a **mean**, never p95. Analytical mode is rejected for percentile SLOs by `statistical_workflow._require_analytical_compatible_slos()` and the CLI check at `kubesizer_cli.py:690`.

## 2.8 Simulation point estimates and uncertainty

For a request class \(k\), the simulation retains the eligible latency arrays by independent replication:

\[
L_{1k},L_{2k},\ldots,L_{Rk}.
\]

The point p95 is the linearly interpolated 0.95 quantile of the pooled eligible observations. It is **not** the mean of per-replication p95 values. For the mean-SLO path, the point estimate is the pooled mean.

Uncertainty uses a replication-cluster percentile bootstrap (`metrics/cluster_bootstrap.py`): sample \(R\) complete replication identities with replacement, concatenate/reweight all observations belonging to those replications, compute the pooled mean or p95, and use bootstrap quantiles as bounds. Whole-replication resampling preserves dependence within a run better than treating every request as independent.

During sequential optimization, the per-tail error probability is:

\[
\alpha_{tail}=\frac{\alpha}{2KC},
\]

where \(K\) is the maximum number of looks and \(C\) is the number of latency plus failure-rate constraints (`CalibratedConfigurationEvaluator._tail_alpha()`, `optimization/simulation_evaluation.py:143`). This is conservative Bonferroni control across tails, looks, and constraints. Final validation has one fixed look and uses \(\alpha/(2C)\).

Terminal failure-rate intervals use a distribution-free independent-replication
KL-Hoeffding bound, not the latency bootstrap. Each replication contributes one
bounded failure fraction. Profile construction rejects a replication ceiling
that could not certify the declared failure-rate SLO even if all observed
requests succeeded.

## 2.9 Optimization objectives

### Replica sizing

The primary objective is weighted replica cost:

\[
J_{replica}(c)=\sum_i a_ic_i,
\]

with default \(a_i=1\). Feasibility has two alternative meanings:

- analytical mode: strict analytical stability, optional structural callback, and every declared **mean** latency estimate below its SLO;
- simulation mode: analytical stability precheck, optional structural callback, and every request-class statistical upper bound below its SLO (plus failure-rate bounds when declared).

Analytical quantities may rank candidates in simulation mode but cannot certify simulated feasibility.

### Node placement

For fixed replica counts, `NodePlacementProblem.evaluate()` (`optimization/node_placement.py:93`) uses:

\[
J_{place}=0.7J_{network}+0.3J_{spread}
\]

unless custom weights are supplied.

For communication-leg rate \(C_{ij}\), source replica fraction \(f_{in}\), destination fraction \(f_{jm}\), and directed one-way node latency \(d_{nm}\):

\[
J_{network}^{raw}
=
\sum_{i,j}C_{ij}
\sum_{n,m}f_{in}f_{jm}d_{nm}.
\]

Ordinary routing contributes one request leg. A synchronous call contributes its outbound request leg and its reverse response leg, matching the simulator's call lifecycle. The cost is normalized by maximum inter-node latency times total communication-leg rate (`NodePlacementProblem.__calculate_network_cost()`, `optimization/node_placement.py`). The separate diagnostic “average network delay” divides raw cost by total external arrival rate.

The spreading penalty is a normalized concentration index based on \(\sum_n f_{in}^2\), averaged over services with more than one replica (`optimization/node_placement.py:1217`). It rewards even spreading. It is not a probabilistic availability or correlated-failure model.

Resource feasibility, when enabled, is simple additive bin packing:

\[
\sum_i c_{in}\,CPU_i\le CPU_n,
\qquad
\sum_i c_{in}\,MEM_i\le MEM_n.
\]

These resource equations are available to library callers only. The active CLI does not enable them and makes no resource-fit claim.

# 3. End-to-end execution flow

## 3.1 Native workload, analytical replica evaluation

1. `kubesizer_cli._load_workflow_system()` loads YAML and builds an open `System`.
2. `System.__init__()` parses classes, distributions, entry streams, service graph, latency components, and execution parameters.
3. The CLI verifies that percentile SLOs are not being sent to analytical mode.
4. `optimize_replica_sizing()` creates `AnalyticalReplicaSizingProblem` (`optimization/replica_sizing.py:174`).
5. The problem computes the componentwise stable vector and bounded search space.
6. `optimize_replica_configuration()` (`optimization/selection.py`) chooses uniform-cost exact search or scalable VNS according to explicit mode or the auto threshold.
7. Every evaluated candidate calls `System.solve()` and `System.get_request_class_mean_latencies()`; no simulator is called.
8. The optimizer returns the lowest found weighted-cost analytically feasible configuration according to its search guarantees.
9. If resources are supplied, placement runs next.
10. The CLI returns only per-service replica counts and optional node counts.

There is no calibration or fresh statistical final validation in analytical mode.

## 3.2 Native workload, simulation replica evaluation

1. Input construction is the same, followed by `System.require_simulation_compatible_workload()`.
2. `statistical_workflow.calibrate()` runs warm-up and measurement-duration calibration and builds an immutable `CalibrationProfile` (`calibration/profile.py:174`).
3. `CalibratedReplicaSizingProblem` (`optimization/replica_sizing.py:838`) defines the bounded state space. The stable vector is the lower bound.
4. Each candidate is first checked analytically. Unstable candidates return `ANALYTICALLY_UNSTABLE` without simulation.
5. `CalibratedConfigurationEvaluator.evaluate()` (`optimization/simulation_evaluation.py:98`) runs 30 replications initially, then batches of 10 up to 60 as needed.
6. Each native replication runs empty-start through frozen \(W\), tags external arrivals during \([W,W+M)\), and drains tagged outcomes.
7. Cluster-bootstrap intervals and failure-rate intervals are computed. The candidate is statistically feasible, statistically infeasible, still sampling, inconclusive, or failed.
8. Exact or scalable replica search consumes only certified-feasible candidates as goals.
9. If placement is skipped, `validate_final_configuration()` immediately executes a fixed 30-replication holdout with fresh seeds.
10. If placement is active, final validation is deferred. The fixed replica vector is placed; node communication and execution multipliers are installed into the simulator; warm-up and measurement duration are recalibrated for that fixed placement; then the fresh 30-replication final validation is run.
11. A failed final validation raises an error; it is not printed as a recommendation.

This sequencing correctly avoids reusing optimization samples for the final reported decision.

## 3.3 Portable evidence flow

1. `_load_workflow_system()` receives `--workload` and `--measurements`; static service facts are embedded under `workload.services`.
2. `load_and_prepare_initial_sizing()` (`characterization/workflow.py:45`) parses and fingerprints all evidence.
3. `characterize_deployment()` derives flows, CPU demand, capacity evidence, latency components, and quality diagnostics.
4. `recommend_initial_replicas()` creates the telemetry-derived seed.
5. `assess_initial_sizing_risk()` rejects the run if required evidence is insufficient.
6. `build_characterized_system_variants()` (`characterization/simulation.py:454`) creates one or more native `System` variants, including call-order sensitivity variants where necessary.
7. The current CLI refuses ambiguous multiple variants rather than choosing one silently.
8. The selected initial vector enters the same analytical or simulation optimization workflow described above.

The portable path therefore does not simply return \(U/X\) sizing. It uses evidence to seed and constrain the model, then the chosen optimizer determines the final vector.

## 3.4 Placement flow

1. The chosen replica vector is installed in `System`.
2. `Cluster` parses nodes and initializes an internal round-robin placement, but this initial placement is not the optimizer result.
3. `NodePlacementProblem` derives service visit rates and edge call rates from the current system.
4. `optimize_node_placement()` (`optimization/selection.py`) calculates the combinatorial space and selects branch-and-bound or Beam/VNS.
5. A complete candidate is checked for fixed replica totals, optional resources, and optional hard constraints.
6. Its latency-weighted communication and spreading objective is calculated.
7. The best placement is converted to per-node replica counts.
8. In simulation mode, `Cluster.simulation_placement_model()` (`performance_models/cluster.py:57`) passes placement, directed latencies, and execution multipliers to the native simulator before final validation.

# 4. Replica-sizing optimizer audit

## 4.1 State and initial condition

A state is an immutable tuple of positive integers in service-ID order. Both active problems expose a bounded rectangular search space between the analytical stable vector and the configured maxima. The CLI default maximum is 10 replicas per service.

The stable vector is a necessary lower bound. The portable telemetry recommendation, if present, is only a construction seed for scalable search. This distinction is correctly reflected in `AnalyticalReplicaSizingProblem` and `CalibratedReplicaSizingProblem`.

## 4.2 Exact mode

`UniformCostReplicaOptimizer` (`optimization/uniform_cost.py:58`) uses a min-heap ordered by accumulated weighted addition cost. Its neighbors add one replica at one service. Analytical latency or bottleneck utilization is used only to order equal-cost states, so it cannot invalidate cost order.

If all lower-cost states are conclusively evaluated, the first/best feasible cost level proves global minimum weighted replica cost inside the bounded state space. If a lower-cost state is statistically inconclusive, the optimizer retains it in `unresolved_lower_cost_states`, reports `STATISTICALLY_UNRESOLVED`, and does not claim a proof. This is an honest exact-search contract.

The exact implementation has no user-configured evaluation or wall-clock budget. Its practical limit is therefore the auto selector, not an internal early-stop certificate.

## 4.3 Scalable mode

`MultiStartReplicaVNSOptimizer` (`optimization/scalable_replica.py:61`) implements:

1. start at the stable vector or higher telemetry construction seed;
2. enumerate valid one-replica additions;
3. rank additions by predicted improvement per cost;
4. select randomly from a restricted candidate list;
5. stop construction at a certified feasible state;
6. remove replicas while feasibility remains certified;
7. perturb by adding one to three replicas and re-prune (VNS);
8. repeat from multiple randomized starts and keep the cheapest certified incumbent.

Defaults in `optimization/mode.py` are 64 unique configuration evaluations, 300 seconds, restricted-list width 8, fraction 0.10, four restarts, three no-improvement restarts, and VNS neighborhood up to three. The random seed defaults to zero.

In analytical mode, ranking is the reduction in worst normalized mean-SLO violation per replica cost. In simulation mode, ranking uses analytical bottleneck utilization because analytical means do not certify simulation feasibility. The evaluator's FEASIBLE/INFEASIBLE/UNKNOWN semantics are preserved. An unknown deletion prevents deletion-local certification.

The heuristic can return a local basin or simply the best state found before its budget. It has no approximation ratio and never sets `global_optimal=True`.

## 4.4 Auto mode and caching

`optimization/selection.py` calculates the bounded replica configuration count:

\[
|\mathcal C|=\prod_i(c_i^{max}-c_i^{stable}+1).
\]

Auto uses exact search at or below 10,000 configurations and scalable search above it (`optimization/mode.py`). The threshold can be calibrated by runtime sampling in `runtime_estimation.calibrate_replica_configuration_limit()`.

Analytical problems cache calculations in memory. The heuristic evaluation controller also avoids repeated evaluation of the same immutable state during one optimizer execution. `CalibratedReplicaSizingProblem.configuration_evaluations`, however, explicitly says entries are run evidence and “never reused as simulation cache.” No persistent cache is used across CLI invocations, topologies, or separate calibration runs.

## 4.5 Guarantees

- Exact analytical mode can prove the bounded weighted-cost optimum for the analytical feasibility model.
- Exact simulation mode can prove the bounded weighted-cost optimum only when every relevant lower-cost statistical result is conclusive.
- Scalable mode returns the best certified feasible incumbent it found and may certify single-deletion local minimality after a complete conclusive deletion pass.
- No scalable result has a global or approximate optimality guarantee.
- None of these optimization guarantees says that the model is an accurate representation of a real deployment.

No retained exact-versus-scalable benchmark establishes solution-quality gaps or runtime scaling for the current optimizer. **NOT ESTABLISHED FROM CURRENT EVIDENCE.**

# 5. Node-placement optimizer audit

## 5.1 State, constraints, and objective

A placement is represented as node → service → replica count. Service totals are fixed by replica sizing. The canonical internal form is a tuple of nodes and sorted service/count pairs (`NodePlacementEvaluation`, `optimization/node_placement.py:13`).

Hard checks available in the problem are:

- exact preservation of each service's replica total;
- nonnegative integer counts and known services/nodes;
- optional CPU and memory capacity, but only for library callers that supply a request for every service;
- an optional user-provided hard constraint callback.

Affinity, anti-affinity, zones, topology-spread constraints, taints, node selectors, pod overhead, daemonset reservation, and disruption budgets have no built-in schema. They could only be encoded through the generic callback in library use.

The active objective combines expected communication latency and a service-replica concentration penalty. Communication frequencies are incorporated for probabilistic routing and explicit call graphs. Routing transitions contribute their directed request delay. Synchronous calls contribute both outbound and return delay. An input containing only visit ratios does not identify source-to-destination edges, so placement omits that communication structure.

The objective is not application end-to-end latency. It assumes traffic is distributed in proportion to replica counts and adds expected directed node latency per observed/modelled edge. It does not model Kubernetes service routing skew, connection reuse, locality policies, congestion, link bandwidth, packet loss, or zone-failure probability.

## 5.2 Exact mode

`BranchAndBoundNodePlacementOptimizer` (`optimization/branch_and_bound_node_placement.py:52`) assigns a complete per-service distribution over nodes. It uses:

- partial CPU/memory feasibility when enabled;
- an admissible communication/spreading lower bound for partial states;
- incumbent pruning.

If it exhausts its search, it can establish the exact optimum for the implemented bounded placement objective.

## 5.3 Scalable mode

`BeamVNSPlacementOptimizer` (`optimization/scalable_node_placement.py:70`) is exposed under the compatibility name `ScalableNodePlacementOptimizer`. Defaults include beam width 32, diversity cap two per signature, at most 64 distributions per service, 100,000 partial states, 10,000 complete evaluations, 100 local iterations, 30 seconds, four restarts, and LNS groups of three, four, and five replicas.

Its phases are:

1. order services/replicas by resource pressure, communication degree, fit scarcity, and spreading difficulty;
2. construct diverse partial placements with a beam;
3. apply best-improving single-replica relocation;
4. apply pairwise swaps and return to relocation after improvement;
5. run small large-neighborhood repairs;
6. restart with diversified order/ties and retain the best feasible result.

It reports relocation/swap local certificates only after complete scans. It never claims global optimality. Beam exhaustion means no placement was found within the heuristic, not proof of infeasibility.

## 5.4 Active-path limitations

Normal CLI placement is intentionally resource-unconstrained. `resource_feasibility_checked` is false, and the result must be interpreted only as a topology/network recommendation. Kubernetes scheduling feasibility is outside this version's claim; a returned placement may be unplaceable if real pod requests or other scheduling constraints are later imposed.

There is also no retained real or generated benchmark comparing placement costs with branch-and-bound optima, no measured network-latency improvement after deployment, and no resilience experiment. Thus placement correctness for the mathematical toy problems is verified, but placement usefulness is **NOT ESTABLISHED FROM CURRENT EVIDENCE**.

# 6. Simulator audit

## 6.1 Language and architecture

The production simulator is no longer SimPy. It is a mandatory C++20 executable implemented primarily in `native_backend/cpp/src/main.cpp`, built by `native_backend/cpp/build.sh`, and invoked through the JSON/subprocess adapter in `native_backend/backend.py`. Protocol version is 12. The current semantics identifier is exported by `native_backend` and fingerprinted into calibration profiles; its current suffix includes `...non-cpu-blocking-v13`.

One replication uses one central future-event priority queue. Events are ordered by:

\[
(event\ time,\ priority,\ monotonically\ increasing\ sequence).
\]

Priorities are urgent 0, normal 1, and deadline 2. The sequence gives deterministic FIFO ordering at identical time and priority. No thread, coroutine, or process is created per request. Independent replications may run in parallel, but events inside one replication are serial.

## 6.2 Event types

`EventKind` at `native_backend/cpp/src/main.cpp:32` contains:

- `ExternalArrival`;
- `Admit`;
- `ServiceCompletion`;
- `Grant` and `Resume` for processing queues;
- `ConcurrencyGrant` for service admission limits;
- `CallReturn` for synchronous child calls;
- `FinalExit`;
- `RequestTimeout`.

## 6.3 External arrivals and request lifecycle

Each configured class/entry pair is an independent external stream. Interarrival samples use the declared distribution. On `external_arrival()` (`native_backend/cpp/src/main.cpp:901`):

1. the request records class, external-entry time, and measurement eligibility;
2. the network-wide active counter increments exactly once;
3. an optional timeout is scheduled;
4. a root visit is created and admitted immediately at urgent priority;
5. the stream's next external arrival is scheduled.

A request can route repeatedly, follow an ordered workflow, or spawn child calls. Internal visits never change the active end-to-end request counter. A 10,000-visit guard fails a nonterminating path rather than running forever.

On final network exit, optional end-to-end latency components/overhead are sampled, then `final_exit()` records the outcome and decrements active requests exactly once. A timed-out request records a failed outcome at its deadline but its server work continues; active requests decrease only at actual server completion. This distinction prevents a timed-out job from disappearing from resource state.

## 6.4 Service execution, queues, and concurrency

Each service's processing capacity is:

\[
c_i\times p_i.
\]

There are two execution modes:

- **fixed slots:** FIFO waiting followed by an occupied processing slot for the sampled service duration;
- **processor sharing:** every admitted visit progresses, with aggregate capacity divided among active visits and completion managed by virtual work tags.

There is a separate optional concurrency/admission capacity per replica. A visit may therefore wait before admission and later wait for processing. In synchronous call graphs, a parent releases its processing slot after its local service completion but retains its concurrency slot while child calls run. Children may execute sequentially or in parallel. The parent completes only after all relevant returns.

Replica queue models are pooled, independent-random, and independent-round-robin. Independent modes select a replica queue explicitly; pooled mode shares service capacity. Fixed-slot queues are FIFO (`std::deque`).

## 6.5 Routing and network delay

Probabilistic routing draws one next hop, including final exit. Ordered workflows implement expected fractional visits through Bernoulli selection. Call graphs sample integer occurrences around fractional mean child-call counts and support fan-out and repeated calls.

When a placement is present, an outbound one-way delay is sampled/applied from the source node to the child/next service node; synchronous calls also receive a return delay. Service execution time is multiplied by the selected node's execution multiplier. Without a placement, communication uses configured same-node/cross-node distributions and a cross-node probability.

## 6.6 State capture

`StateRecorder` (`native_backend/cpp/src/main.cpp:302`) maintains the fundamental states:

- \(A(t)\): active external requests;
- \(Q_i(t)\): admission waiting + processing waiting; for processor sharing, active visits beyond processor count are represented as backlog;
- \(B_i(t)\): occupied processing slots, capped by capacity.

Histories begin at `(0.0, 0)`, record only changes, and coalesce changes at the same timestamp. Assertions prevent negative active/queue/busy values and busy counts above capacity. Queue and busy areas are integrated as state transitions occur. Derived values are:

\[
N_i(t)=Q_i(t)+B_i(t),\qquad U_i(t)=B_i(t)/(c_ip_i).
\]

The Python `SimulationMetricsCollector` in `metrics/simulation_metrics_collector.py` validates and exposes native histories. Exact online buckets are also supported inside C++; the independent pure Python reference bucketizer is `metrics/time_weighted_buckets.py`.

## 6.7 Termination and measurement semantics

Fixed-horizon mode processes events strictly before the horizon and may finish with requests in flight.

Measurement-drain mode follows:

```text
0 ---------------- W ---------------- W + M ---------------- drain limit
       warm-up          tagged arrival cohort          finish outcomes
```

- The system begins empty at time zero.
- Nothing resets at \(W\).
- Requests arriving before \(W\) continue to affect state but are excluded from latency samples.
- Requests arriving in \([W,W+M)\) are tagged.
- Arrivals after \(W+M\) continue as background while tagged outcomes drain, but are not tagged.
- The run stops when every tagged request has either completed or timed out.
- It fails if the configured maximum drain time is exceeded.

This avoids right-censoring slow successful eligible requests. A deadline outcome is deliberately complete for failure/latency classification even when the underlying server work remains active.

## 6.8 Randomness and replications

Each native replication uses `std::mt19937_64` with its supplied signed-64-bit seed. Same seeds reproduce the same native path. Different languages in the historical benchmark use different standard generators, so cross-language stochastic traces are compared distributionally rather than event-for-event.

Python launches independent native replication subprocesses through a thread pool. Threads coordinate processes; they do not parallelize one replication. Warm-up ensemble mode can instead execute multiple replications and reduce exact buckets inside one native process with C++ worker threads.

## 6.9 Model assumptions versus implementation choices

**Model assumptions:** stationary external distributions; supplied request paths represent actual calls; supplied service work represents the capacity-limiting resource; fixed replica capacities; no retries unless explicitly encoded as calls; no priority classes; no database/disk/connection-pool resource unless represented; node delay independent of load; declared timeout semantics.

**Implementation choices:** event priority ordering; 10,000-visit guard; pooled/independent queue selection; holding concurrency but not processing capacity during synchronous calls; uniform replica selection implied by queue/placement mode; linear p95 interpolation; empty-start replications.

**Experimental parameters:** \(W\), \(M\), seed blocks, number of replications, bootstrap draws, processor count, bounds, and optimizer budgets. These belong in a calibration profile or experiment manifest, not in the simulator's universal semantics.

# 7. Calibration and statistical methodology

## 7.1 Calibration profile and invalidation

`CalibrationProfile` (`calibration/profile.py:174`) is frozen and YAML-serializable. It contains topology/workload fingerprints, simulator-semantics version, bounded replica space, \(W\), \(M\), bucket/window choices, replication policy, SLOs, CI method, seed policy, and diagnostics. `experiment_fingerprints()` (`calibration/profile.py:674`) includes service identities, routes/calls, rates, distributions, request classes, SLOs, latency components, execution semantics, and bounds. A replica change inside the calibrated bounds does not invalidate the profile; relevant workload/topology/semantics changes do.

The active CLI does not save or reload the profile. Reuse is available only through explicit library serialization calls. Therefore calibration is currently repeated on each simulation CLI run. If node placement is enabled, the CLI also creates a second profile for the selected fixed placement before final validation. The placement model is fingerprinted, so an unplaced profile or a profile for a different placement is rejected.

## 7.2 Warm-up pilot

Default `CalibrationOptions` (`calibration/profile.py:25`) uses:

- 30 minimum-configuration pilot replications;
- 100 validation replications per representative configuration;
- final 20% reference region;
- 10 buckets per analysis window;
- three consecutive stable windows;
- 5% relative tolerance;
- 10% horizon and bucket-width sensitivity tolerance;
- one safety window;
- up to ten horizon doublings.

The initial bucket width is five times the slowest mean service time. Candidate widths are half, equal, and double that value. The initial horizon is the maximum of 100 congestion time scales, 1,000 expected external arrivals, and 500 base buckets (`calibration/warmup_parameters.py:160-196`). These are sizing heuristics only.

There is a small formulation discrepancy: this warm-up heuristic calculates slack as `replicas * service_rate - arrival_rate`, whereas the main stability calculation uses processing slots and a class-specific service mixture. The heuristic may therefore size \(H\) inconsistently for non-default processing slots or heterogeneous class demands. Subsequent empirical checks can reject the horizon, but the initial estimate is still inconsistent.

## 7.3 Exact bucket construction and Welch ensemble

For a piecewise-constant state history, `time_weighted_bucket_averages()` computes:

\[
Y^X_{rj}=\frac1\Delta\int_{j\Delta}^{(j+1)\Delta}X_r(t)dt.
\]

It integrates exact interval intersections, flushes the final state to the horizon, and discards an incomplete final bucket. Boundary snapshots are not used. The ensemble series averages corresponding buckets:

\[
\bar Y^X_j=\frac1R\sum_rY^X_{rj}.
\]

Metrics are active requests and per-service queue length, number in system, and utilization. Busy count is used to derive utilization but is not treated as independent convergence evidence.

## 7.4 Cutoff rule

For each metric, the last 20% of the ensemble defines reference \(\mu_{ref}\). Tolerance is:

\[
\max(0.05|\mu_{ref}|,\epsilon_{abs}),
\]

with absolute floors of 0.1 request for count metrics and 0.01 for utilization. Non-overlapping window means are inspected. The first three consecutive windows all within tolerance are accepted only if their fitted trend changes by no more than one tolerance over the three-window span. The metric cutoffs are maximized and one safety window is added (`calibration/cutoff.py`). A metric that never settles fails calibration.

`calibrate_minimum_configuration()` (`calibration/workflow.py:349`) runs the minimum stable configuration and doubles \(H\) until two consecutive adequate horizons agree within 10%. One long native trajectory is reused for its shorter prefix and rebucketed for compatible widths. It also requires \(W\le H/3\), at least 50 post-cutoff buckets, stable 60–80% versus 80–100% late means, and adjacent-width agreement.

## 7.5 Global warm-up validation

Representative configurations include minimum, quartiles, maximum, and optional SLO-boundary cases (`build_representative_configurations()`, `calibration/validation.py:41`). Fresh validation seeds are used. The proposed global \(W\) is the maximum required cutoff, with another safety window if validation raises it.

`validate_global_warmup()` computes `post_warmup = check_post_warmup_stability(...)` and accepts each representative configuration only when both conditions hold:

```python
passed = (
    cutoff.warmup_duration <= proposed_warmup
    and post_warmup.passed
)
```

This enforces both an early-enough estimated cutoff and stability of the actual first post-\(W\) windows (`calibration/validation.py`). A regression test rejects a configuration whose inferred cutoff is early but whose post-\(W\) windows still fail the stability check.

## 7.6 Measurement duration

The initial duration uses the slowest class external rate:

\[
M_0=1.2\max_k\frac{2000}{\gamma_k}.
\]

Candidates are \(M_0,2M_0,\ldots,64M_0\) by default. The final value is used as the doubling comparison for the largest selectable value, so the largest accepted default duration is \(32M_0\). Thirty replications are used for each representative configuration. Prefix arrival cohorts allow a longer drained replication to be analyzed for shorter candidate windows.

For every class, acceptance requires at least 20,000 eligible requests in total, at least 20 nonempty replications, CI half-width below the larger practical precision allowance, at most 5% statistic change after doubling, and stable CI width within 10%. The global \(M\) is the maximum accepted across representative configurations (`calibration/measurement.py:353`).

The implementation reuses the first \(2M_0\) run for the first pair, then may run the longest duration again for later pairs rather than running one longest trajectory once. This is a performance inefficiency, not a statistical correctness defect.

## 7.7 Sequential evaluation and final validation

Defaults are \(R_{min}=30\), \(R_{max}=60\), batch size 10, so maximum looks \(K=4\). Classification at a look is:

- feasible when every upper bound is at or below its SLO;
- infeasible when any lower bound is above its SLO;
- continue sampling otherwise before \(R_{max}\);
- inconclusive otherwise at \(R_{max}\).

Missing minimum sample counts cause continued sampling and then explicit simulation failure at the maximum. Inconclusive is never accepted as feasible.

Final validation locks the selected configuration and runs exactly 30 fresh-seed replications with no early stopping (`optimization/replica_validation.py:144`). Every familywise-adjusted upper bound must pass.

## 7.8 Seed policy

`calibration/seeds.py:121` defines disjoint ranges:

| Namespace | Range |
|---|---:|
| warm-up calibration | 100,000–999,999 |
| warm-up validation | 1,000,000–9,999,999 |
| measurement calibration | 10,000,000–99,999,999 |
| optimization | 100,000,000–199,999,999 |
| bootstrap | 200,000,000–999,999,999 |
| final validation | 1,000,000,000–1,099,999,999 |

Within optimization, the same replication index uses the same seed across configurations (common random numbers), while replications within one configuration remain independent. Bootstrap seeds are deterministic hashes in their own namespace.

## 7.9 Statistical limitations

- Percentile bootstrap is a percentile cluster bootstrap; it is not a studentized or bias-corrected interval and its coverage has not been empirically calibrated for these workloads.
- Default bootstrap resolution can reject many-class workloads. With \(K=4\), 5,000 draws, alpha 0.05, and the five-tail-draw minimum, more than about six total constraints cannot meet the configured tail-resolution check. This fails clearly rather than silently, but the default is not usable for approximately 20 request classes without more resamples or a different multiplicity method.
- The warm-up earliest-window detector showed substantial seed sensitivity at 30 and 100 replications in retained benchmark evidence.
- The claim that one global \(W\) is valid over all bounded configurations assumes adding replicas does not change routes, retries, service work, timeouts, concurrency behavior, or other semantics.
- An in-memory current-v13 replay demonstrates full calibration, exact optimization, and fresh final validation for a retained characterized model. It is not a live current-revision deployment experiment and no serialized current profile was retained.

# 8. Retained experiment inventory

## 8.1 Artifact locations and provenance

The current Kubesizer Git repository does **not** retain raw experiment results or generated plots. It contains example CSV/YAML fixtures, reporting code, and an older technical report, but these are not raw scientific evidence for this audit.

At the original audit time, meaningful evidence was found in two sibling repositories and one completed remote study:

- simulator benchmark: `/home/ktistos/work/queue-simulator-benchmark/results/`;
- deployment/load experiments: `/home/ktistos/work/STELAR_LOAD_TESTS/experiments/`;
- completed deployment-family study: formerly `experiment@46.224.93.195:/home/experiment/experiments/deployment-family-sizing-validation-20260827/`.

The benchmark repository was clean at commit `91f8258` during this audit. The local load-test repository was **dirty** at commit `e892537`; at least `stelar_measure/sizing_analysis.py` and tests had uncommitted modifications. The completed remote deployment-family manifest recorded load-test commit `5a192e0` and Kubesizer commit `572d338`, but both worktrees were dirty and the manifest stored only the list of changed paths, not the diff or content hashes. The study also predates the current audit base `6d223c1`.

**Post-audit preservation warning.** The Hetzner experiment VM was subsequently deleted without a volume or snapshot. The complete 405-treatment run directories, Locust traces, VM telemetry, and original remote analysis files are no longer available. A 46.4 MB local recovery bundle at `STELAR_LOAD_TESTS/experiments/recovered-rich-sizing-20260830/` preserves Kubernetes objects, logs, cumulative ingress/Istio counters, Prometheus exports, and SHA-256 checksums. Time-series traffic survives meaningfully for only the blocking-pool and repeated-mixed namespaces; it is not a full recovery of the deployment-family campaign. The machine-readable `results/richer-simulation-validation.yaml` preserves the selected accepted observations, source hashes, model inputs, simulation seeds, and replay results used by E15. Consequently, the previously inspected E11 aggregate is reported for transparency but is no longer independently reproducible from complete raw data.

This provenance distinction matters: a deployment observation remains valid as a cluster observation, but it cannot be claimed as exact validation of today's source without reconstructing the dirty snapshot.

## 8.2 Inventory table

The table groups directories that are repetitions or stages of one scientific question. Individual smoke, interrupted, expired-authentication, and explicitly invalid carryover directories are not promoted to separate experiments.

| Experiment ID | Question / hypothesis | Independent variables | Controlled variables | Metrics | Compared methods | Dataset / topology / workload | Runs / replications | Raw data location | Processing script / artifact | Plot/table | Observed result | Interpretation | Status |
|---|---|---|---|---|---|---|---:|---|---|---|---|---|---|
| E01 | Does a custom/compiled event kernel justify replacing SimPy for calibration workloads? | Language/kernel; capture on/off; sequential/parallel mode; topology | Frozen event semantics, workload JSON, worker count, benchmark protocol | wall/CPU time, events/s, requests/s, RSS, speedup, capture overhead | SimPy, custom Python, Java, Go, C++ | Seven synthetic stochastic workloads including `thesis_representative` | 5 untimed warm-ups + 20 timed samples per cell; 30-replication batch modes | `queue-simulator-benchmark/results/raw/phase1-full.jsonl`, `phase2-full.jsonl`, corrected C++ batches, memory JSONL | `benchmark/analyze_results.py`; `results/report.md`; summary JSON/CSV | Runtime and memory tables | C++ was 35.65× custom Python in the key batch/capture mode; all frozen deterministic traces passed | Supports the C++ performance decision for the frozen kernel, not predictive validity | **VERIFIED RESULT** |
| E02 | Is long-horizon warm-up simulation computationally feasible in C++? | Custom Python vs C++; horizon 16,000; exact bucketing | thesis workload, \(R=30\), \(\Delta=0.5\), 8 workers | kernel time, cutoff, exact bucket equality | custom Python, C++ | benchmark thesis workload | One measured batch per implementation; six deterministic fixtures | `queue-simulator-benchmark/results/raw/long-calibration-buckets.json.gz` | `results/calibration-validation.md`, `long-calibration-validation.json` | Runtime and cutoff table | 60.809 s vs 1.913 s (31.8×); six bucket fixtures matched | Directly confirms speed feasibility, but one timing sample per implementation is weak for runtime variance | **VERIFIED RESULT** |
| E03 | How seed/replication count affects warm-up cutoff stability | \(R=30,100,300\); independent seed blocks; \(H\) vs \(2H\) | C++ kernel, thesis workload, \(\Delta=0.5\), 8 workers | candidate \(W\), horizon adequacy, H/2H sensitivity, RSS, runtime | replication counts | benchmark thesis workload | 5 ensembles each for 30/100; 3 for 300 | `queue-simulator-benchmark/results/raw/` sensitivity captures | `cpp-calibration-ensemble-sensitivity*.json/.md` | Summary tables | H/2H pass: 80% at R=30, 20% at R=100, 100% in 3 R=300 ensembles; R=300 \(W\) 60–120 | Exposes detector seed sensitivity; three R=300 ensembles are too few to establish a universal replication choice | **PRELIMINARY RESULT** |
| E04 | Is the frozen analytical mean-latency model sufficient as the sole SLO oracle for STELAR? | Offered rate 4,16,32,48,56 RPS | one-replica vector, 3:1 operation mix, frozen visits/demands/residuals | app/client mean, p95, failures, flow error, demand change | analytical prediction vs live system; constant baseline | STELAR metadata-read | 3 runs at 4 and 56; 1 at intermediate rates | `STELAR_LOAD_TESTS/experiments/analytical-sufficiency/2026-08-21/` | `scripts/analyze_analytical_sufficiency.py`; `analysis.yaml`; `report.md` | Result table and sensitivity table | near capacity predicted 330.658 ms vs observed 1538.487 ms; false feasible; max flow error 1.44% | Mean flow was accurate while latency/capacity was not; aggregate U/X is insufficient to identify the bottleneck | **VERIFIED RESULT** |
| E05 | Do operation-specific demands, clustering, and low-load residuals fix the STELAR analytical forecast? | operation, load point, fresh holdout run | deployment, characterization method, fixed low-load residual | mean prediction error, failures, flow error, stationarity | enriched analytical model vs live cluster | STELAR `dataset_search` and `organizations`; one descriptive cluster | 2 runs/operation at 3 characterization rates; 3 fresh mixed holdouts | `STELAR_LOAD_TESTS/experiments/operation-characterization/` and `cluster-holdout/...w300*` | `scripts/analyze_operation_profiles.py`, `validate_cluster_model.py`; combined `report.md` | operation and holdout tables | combined status FAILED; one 5.998 s pathological holdout and two recovery means 194.355/189.624 ms vs 178.032/181.068 ms; post-warm-up ensemble failed | Enrichment improved healthy-run errors but did not yield defensible stationarity or universal capacity prediction | **VERIFIED RESULT** |
| E06 | Does a synchronous-call simulator with calibrated path residual generalize to fresh STELAR runs? | holdout replication | one characterization run for residual, fixed model, rate ≈48 RPS | predicted vs observed client/app mean, bootstrap prediction interval, failures | native simulation vs live runs | STELAR clustered metadata-read, one configuration | 1 residual-fit run; 2 validation observations; 30 simulation reps per observation | `STELAR_LOAD_TESTS/experiments/predictive-model/2026-08-23-synchronous-root-holding/` | `scripts/validate_simulation_model.py`; `predictive_validation.yaml` | YAML diagnostics | client errors 1.85% and 3.76%; both passed stored rule | Encouraging narrow holdout; residual has only one observation and simulator semantics are v7, not current v13 | **PRELIMINARY RESULT** |
| E07 | Is a proposed STELAR replica vector reliable? | candidate/lower configurations and repeat | target workload fixed | mean latency, failures, prediction error | simulator recommendation vs deployed configurations | STELAR sizing validation | 3 candidate reps plus characterization/lower runs | `STELAR_LOAD_TESTS/experiments/sizing-validation/` | predictive-validation artifact | YAML only | one candidate repetition had about 49.98% failures; stored validation failed | Cannot validate the recommendation; evidence includes a material negative result | **INCOMPLETE** |
| E08 | What is the empirical service capacity envelope? | worker replica count and offered rate | synthetic chained workload | throughput, latency, failures, CPU, traces | configurations/rates | controlled synthetic chain | 9 completed run directories | `STELAR_LOAD_TESTS/experiments/capacity-envelope-20260826/` | No consolidated retained analysis found | Raw CSV/YAML only | Individual raw runs exist; no audited aggregate conclusion | Useful data, but no retained decision analysis | **INCOMPLETE** |
| E09 | Do deterministic and lognormal service distributions behave differently across replica counts? | service distribution; worker replicas | synthetic topology and load protocol | raw latency, CPU, errors, traces | deterministic vs lognormal configurations | synthetic chain | 6 deterministic plus at least 17 lognormal run directories | `STELAR_LOAD_TESTS/experiments/deterministic-*`, `lognormal-*`, `kubesizer-evaluation-20260826/`, `synthetic-chain/` | No single authoritative retained summary found | Raw artifacts | Data exist, but experiment boundaries and accepted/excluded runs are not sufficiently documented | No defensible aggregate result reconstructed | **UNCLEAR** |
| E10 | Does the simulator recommend a feasible replica vector for a controlled heavy-tail system, and are cheaper neighbors safe? | recommended, cheaper backend, worker-limited configs | 24 RPS exponential arrivals, lognormal service SCV 4, 150 ms mean SLO | run means, t/bootstrap CIs, failures, window violations | frozen simulator prediction vs deployed synthetic system | three-service synchronous chain | recommended 5; cheaper 20; worker-limited 5 | `STELAR_LOAD_TESTS/experiments/heldout-sizing-20260826/` | retained `summary.yaml` | Configuration table | recommendation [1,2,2] mean 116.706 ms, CI [105.129,128.284], feasible; cheaper [1,2,1] also mean-feasible but 4/20 windows violated; worker-limited mean 1.164 s | Validates feasibility for one controlled topology, but recommendation was conservative for expected mean and not proven minimal | **VERIFIED RESULT** |
| E11 | Does sizing generalize across deployment families and load shapes? | four families, five load points including bursty, bounded config grids | 60 s warm-up, 120 s cohort, 60 s drain, seed plan, cluster, evidence gates | mean latency CIs, prediction error, failure, arrival fidelity, scheduler quality, empirical minimum in tested set | Kubesizer recommendation vs live candidate grid | single-service, fan-out, repeated visit, shared multiclass | 405 completed treatments; normally 5 reps, target validation 30 | Original remote VM deleted; partial cluster-side recovery under `STELAR_LOAD_TESTS/experiments/recovered-rich-sizing-20260830/` | original dirty-snapshot scripts and analysis no longer retained; selected observations preserved in E15 artifact | Derived tables only; incomplete recovery | Previously inspected aggregate: 12/20 evidence rejected; accepted: 6 matches, 2 underprovisioned; target validation: 2 families pass, 2 fail | Scientifically informative, but complete raw evidence and exact dirty source snapshot are no longer reproducible | **PRELIMINARY RESULT** |
| E12 | How close is scalable replica VNS to exact uniform-cost search? | generated instances, seed, budget | evaluator/problem fixtures | objective gap, optimum hit, runtime/evaluations | exact vs scalable | small synthetic unit fixtures | Unit tests only; no retained benchmark matrix | `tests/test_scalable_replica_optimizer.py`, `test_optimization_modes.py` | Test suite | Assertions only | Counterexamples and deterministic behavior are tested; population-level quality is not measured | Only implementation behavior is verified; optimizer-quality distributions remain unmeasured | **IMPLEMENTED BUT NOT RUN** |
| E13 | How close is scalable placement to exact branch-and-bound and does placement improve deployed latency? | beam/VNS parameters, topology, placement | fixed replicas | objective gap, runtime, deployed latency | exact vs scalable; before/after | synthetic test fixtures only | Unit tests only | `tests/test_node_placement.py` | Test suite | Assertions only | Algorithm mechanics pass; scientific placement benefit is unmeasured | No scientific placement-quality or deployed-latency claim is supported | **IMPLEMENTED BUT NOT RUN** |
| E14 | Does the complete current v13 calibration → optimization → final-validation workflow work on a real deployment? | topology/workload/configurations | current source and defaults | accepted \(W,M\), looks, CIs, final decision, runtime | current workflow vs live deployment | none retained at exact audited revision | None | None | None | None | No clean current-revision artifact exists | The current end-to-end workflow remains externally unvalidated | **IMPLEMENTED BUT NOT RUN** |
| E15 | Does current v13 reproduce retained family decisions and complete its public workflow on a characterized family? | five simulator semantics; one target family and replica bound | retained low-load characterization; 30 replications; fixed seeds | mean latency error, decision agreement, accepted \(W,M\), recommendation, fresh CI | baseline and four enriched models | four retained target-validation families; single-family end-to-end replay | 30 simulation reps per point/model; 30 final-validation reps | `results/richer-simulation-validation.yaml`; local retained characterization artifacts | `scripts/validate_richer_simulation.py`; in-memory public workflow | Console results recorded in this report | safest portable model: 3/4 target decisions, zero unsafe false-feasible, 28.1% MAPE; single-family workflow selected 5 replicas and fresh validation estimated 90.006 ms [89.376, 90.666] under 100 ms | Confirms current code runs conservatively on retained evidence; five replicas were not deployed in the retained experiment, so recommendation accuracy is unverified | **PRELIMINARY RESULT** |

## 8.3 Number of verified experiments

Using the evidence that remains independently auditable, there are **five VERIFIED RESULT experiment families**: E01, E02, E04, E05, and E10. This count includes negative experiments and treats the cross-language benchmark and direct long-horizon run as different questions. E03, E06, E11, and the current retained-data replay E15 are preliminary. E11 was downgraded because its complete remote raw data and exact dirty source snapshot were lost when the VM was deleted.

# 9. Reconstructed measured results

## 9.1 Simulator performance

The cross-language benchmark froze a simpler common event semantics than today's rich v13 backend. All five implementations passed six deterministic traces; the compiled languages also passed fixed-seed reproducibility and distributional checks across seven stochastic workloads. Every primary runtime cell used five untimed warm-ups and 20 timed samples with shuffled order.

For 30 state-capturing replications on eight workers:

| Workload | SimPy median | Custom Python median | Java median | Go median | C++ median |
|---|---:|---:|---:|---:|---:|
| small topology | 0.1193 s | 0.0533 s | 0.0191 s | 0.0125 s | 0.0015 s |
| medium topology | 0.6246 s | 0.2910 s | 0.0848 s | 0.0482 s | 0.0087 s |
| large topology | 1.5144 s | 0.7113 s | 0.1302 s | 0.0975 s | 0.0180 s |
| high utilization | 0.6946 s | 0.3676 s | 0.0987 s | 0.0656 s | 0.0113 s |
| routing loops | 0.4004 s | 0.1885 s | 0.0632 s | 0.0316 s | 0.0047 s |
| thesis representative | 4.6826 s | 2.4683 s | 0.3024 s | 0.3207 s | 0.0692 s |

For the thesis representative case, speedup over custom Python was:

| Candidate | Median speedup | Bootstrap 95% interval |
|---|---:|---:|
| Java | 8.16× | [8.03×, 8.29×] |
| Go | 7.70× | [7.60×, 7.74×] |
| C++ | 35.65× | [34.82×, 36.20×] |

Median process-tree peak RSS for that parallel capture mode was 511.7 MiB for SimPy, 434.4 MiB for custom Python, 411.2 MiB for Java, 119.4 MiB for Go, and 43.9 MiB for C++.

**Justified conclusion:** replacing the frozen Python event kernel with C++ makes repeated calibration simulations materially faster and less memory-intensive on the benchmark machine.

**Not justified:** the benchmark does not show that C++ predictions are more accurate, that the current v13 semantics match SimPy, or that the statistical cutoff rule is correct.

## 9.2 Direct calibration runtime and cutoff sensitivity

At \(H=16{,}000\), \(R=30\), \(\Delta=0.5\), and eight workers, the direct measured runtime was 60.809 seconds for custom Python and 1.913 seconds for C++. This is a single batch timing per implementation, so “31.8× on this run” is defensible; a narrow confidence interval is not.

The seed-sensitivity study found:

| Replications per ensemble | Independent ensembles | Median C++ kernel time at H=32,000 | H/2H cutoff agreement | Candidate-W summary |
|---:|---:|---:|---:|---:|
| 30 | 5 | 2.730 s | 80% | H/2 median 1010; H median 1010; broad spread |
| 100 | 5 | 8.930 s | 20% | H/2 median 220; H median 280; broad spread |
| 300 | 3 | 26.207 s | 100% | both horizons median 70, range 60–120 |

The non-monotonic 30-versus-100 result shows that a small number of meta-replicates cannot select \(R\) reliably. Three successful R=300 ensembles are encouraging but insufficient as a universal rule.

## 9.3 STELAR analytical sufficiency

The most informative negative analytical result is:

| Configured RPS | Runs | Analytical app mean | Observed app mean | Relative error | Client failure rate |
|---:|---:|---:|---:|---:|---:|
| 4 | 3 | 66.879 ms | 66.591 ms | +0.43% | 0.000% |
| 16 | 1 | 71.110 ms | 66.015 ms | +7.72% | not retained in table |
| 32 | 1 | 81.537 ms | 72.295 ms | +12.78% | not retained in table |
| 48 | 1 | 117.600 ms | 104.217 ms | +12.84% | not retained in table |
| 56 | 3 | 330.658 ms | 1538.487 ms | −78.29% | 0.496% |

Maximum service-flow prediction error was only 1.44%, while maximum effective-demand change was 70.77%. This separates two claims:

- the mean traffic/visit model described service flows well in this experiment;
- it did not identify the load-dependent latency/capacity boundary.

The result supports using analytical equations as a stability screen and ranking aid, not as the sole real-system mean-SLO oracle for this workload.

## 9.4 Operation-aware analytical holdout

Operation-specific characterization produced healthy-load analytical errors of 17.59% for `dataset_search` and 22.39% for `organizations` at 32 RPS. At 56 RPS, `organizations` remained analytically stable with a 152.791 ms prediction but observed 4929.329 ms and 27.57% failures.

The three fresh mixed-operation holdouts were:

| Holdout | Predicted mean | Observed mean | Error | Failures | Stationarity/SCV gate |
|---|---:|---:|---:|---:|---|
| original rep 1 | 179.743 ms | 5997.821 ms | −97.00% | 6.18% | failed |
| recovery rep 1 | 178.032 ms | 194.355 ms | −8.40% | 0.01% | failed |
| recovery rep 2 | 181.068 ms | 189.624 ms | −4.51% | 0.00% | failed |

The two recovery observations are close in ordinary percentage terms, but the predeclared post-warm-up ensemble gate failed. The scientific result is therefore FAILED, not “the model is accurate within 10%.”

## 9.5 Preliminary synchronous-call simulation holdout

One run estimated an external path component as client mean minus application-entry Istio mean: 32.296 ms. Two fresh observations at approximately 48 RPS were then compared with 30 simulation replications each:

| Holdout | Observed client mean | Simulated mean | Simulation interval | Relative error |
|---|---:|---:|---:|---:|
| rep 2 | 152.193 ms | 155.014 ms | [152.290, 157.557] ms | +1.85% |
| rep 3 | 151.278 ms | 156.966 ms | [154.172, 159.740] ms | +3.76% |

The underlying artifact explicitly identifies simulator semantics v7 and says the residual had one calibration observation, with no run-to-run variance estimate. This is good narrow evidence that synchronous caller holding plus a frozen path residual can transfer to two nearby runs. It does not establish transfer to another topology, load, replica vector, p95 SLO, or current semantics v13.

## 9.6 Controlled heavy-tail sizing holdout

The workload used exponential 24 RPS arrivals, lognormal service times with SCV 4, synchronous sequential calls, 60 seconds warm-up, 180 seconds measurement-arrival window, and 60 seconds drain.

| Configuration | Real replications | Eligible requests | Mean latency | 95% t interval | Failures | Frozen simulated mean | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| recommended [front 1, worker 2, backend 2] | 5 | 21,318 | 116.706 ms | [105.129, 128.284] ms | 0 | 134.888 ms | feasible under 150 ms mean SLO |
| cheaper [1,2,1] | 20 | 86,977 | 131.637 ms | [122.043, 141.231] ms | 1 timeout | 149.250 ms | pooled mean feasible, but 4/20 windows violated |
| worker-limited [1,1,2] | 5 | 21,369 | 1164.207 ms | [743.842, 1584.572] ms | 4 timeouts | not listed in summary | decisively infeasible |

The recommendation was safe for this held-out controlled workload. It was not minimal for a pooled expected-mean criterion, because the cheaper backend neighbor's upper mean bound also passed. The lower neighbor's finite-window instability illustrates that “expected mean passes” and “every operational window has margin” are different constructs.

## 9.7 Deployment-family sizing study

The completed remote study used four synthetic Kubernetes deployment families, five load points per family, randomized run order/seeds, exponential arrivals except the declared bursty cases, 60 seconds warm-up, 120 seconds cohort, 60 seconds drain, and 20,000 bootstrap resamples. The earlier direct audit counted 405 completed treatments. The complete raw run tree is no longer retained after deletion of the experiment VM; the values below are preserved audit findings, not a presently reproducible reanalysis of all raw treatments.

Quality-gate counts over recorded per-run diagnostics were 353 passing and 52 failing. Failure reasons overlap: 42 load-generator saturation findings, 33 excessive scheduler-delay findings, 30 excessive request-failure findings, and 21 arrival-rate misses. Across all raw response statuses, the analysis saw 2,063,893 HTTP 200 responses, 81,527 HTTP 502 responses, and 22 status-0 outcomes.

At the four 30-replication target-validation points, the artifact's field is named `p95_estimate`, but `latency_statistic: mean`; these numbers are pooled mean-latency estimates, not p95 values:

| Family | Recommended replicas | Simulation prediction | Live mean and 95% cluster interval | Mean SLO | Decision |
|---|---|---:|---:|---:|---|
| single | [3] | 78.793 ms | 106.476 ms [105.243, 107.687] | 100 ms | underprovisioned |
| fan-out | [1,1,2] | 131.287 ms | 151.477 ms [149.488, 153.462] | 140 ms | underprovisioned |
| repeated visit | [1,2] | 92.961 ms | 110.028 ms [109.456, 110.743] | 160 ms | matched tested empirical minimum |
| shared read | [1,2,1] | 138.981 ms | 113.962 ms [112.457, 115.530] | 150 ms | passed |
| shared write | [1,2,1] | 141.348 ms | 116.862 ms [115.136, 118.671] | 160 ms | passed |

Over all 20 family/load-point results, 12 were evidence-rejected, six matched the empirical minimum in the explicitly tested set, and two were underprovisioned. Among evidence-accepted points, this is a 75% match rate and 25% unsafe-underprovision rate. It must not be quoted as a general accuracy percentage because the cases are not an iid sample of production deployments and the source snapshot was dirty.

The strongest sizing conclusion supported by this study is negative but useful: the method distinguishes useful configurations in several call patterns, yet current model form or characterization still underestimates latency enough to reverse an SLO decision in simple and fan-out families.

## 9.8 Current-v13 retained-data replay and complete workflow exercise

The committed replay artifact `results/richer-simulation-validation.yaml` is the strongest machine-readable bridge between the older deployment observations and the current richer simulator. It preserves the selected accepted observations, characterization hashes, model parameters, simulation seeds, and predictions. It does **not** replace the lost complete raw deployment-family dataset.

Across 18 accepted configuration decisions, with 22 class/configuration latency comparisons, the five replayed model variants produced:

| Model | Mean absolute error | MAPE | Feasibility agreement | Unsafe false feasible | Conservative false infeasible |
|---|---:|---:|---:|---:|---:|
| Existing baseline | 29.32 ms | 18.50% | 16/18 (88.9%) | 2 | 0 |
| Telemetry CPU + mean blocking | 77.39 ms | 36.83% | 10/18 (55.6%) | 8 | 0 |
| Telemetry + declared bottleneck pool | 31.24 ms | 23.89% | 15/18 (83.3%) | 3 | 0 |
| Telemetry + fitted blocking variability | 68.39 ms | 43.39% | 17/18 (94.4%) | 0 | 1 |
| Oracle occupancy + blocking | 30.54 ms | 18.32% | 16/18 (88.9%) | 1 | 0 |

The full accepted-point table favors the fitted-variability model for **decision safety**, not numerical accuracy. It removes unsafe false-feasible decisions in this retained set by becoming more conservative. Its MAPE is worse than the baseline. The “oracle” uses privileged synthetic occupancy information unavailable to ordinary DevOps measurement, while the declared-bottleneck models require a supplied worker/thread/connection-pool limit that counters alone do not identify.

On the stronger four-point subset with 30 live replications per target, the fitted-variability model had 3/4 decision agreement, no unsafe false-feasible decision, one conservative false-infeasible decision, and 28.07% MAPE. This is encouraging safety evidence on a tiny selected subset, not a calibrated general error rate.

A separate in-memory exercise used the current public calibration and optimization interfaces on the retained characterized single-service model:

| Workflow stage | Result |
|---|---|
| Warm-up pilot | Horizon doubling continued to 10,242.6 s; paired adequate horizons produced the same cutoff |
| Minimum-configuration warm-up | 67.056 s |
| Fresh representative-configuration warm-up validation | 100 validation replications; accepted global `W=87.918041 s` |
| Measurement-duration calibration | Global `M=1536 s`; the two-replica boundary controlled the duration |
| Exact search with upper bound 4 | No feasible configuration found within the bounded space |
| Exact search with upper bound 5 | Selected configuration `(5,)` |
| Fresh final validation | 30 new seeds; 2,303,587 eligible requests; mean 90.006389 ms; simultaneous bounds [89.375614, 90.665816] ms under a 100 ms mean SLO |

This exercise establishes that the corrected current workflow can calibrate, reject an insufficient bound, optimize, and perform fresh-seed final validation without fabricating feasibility. It establishes only **model-conditional simulated feasibility**. The retained live study tested no five-replica configuration, so the recommendation's deployed feasibility and minimality remain **NOT ESTABLISHED FROM CURRENT EVIDENCE**.

## 9.9 Results not established

No retained artifact supports numerical claims for:

- current v13 p95 prediction error on a real deployment;
- a successful current default warm-up \(W\) and measurement \(M\) profile for the root workload;
- exact-versus-scalable replica objective gap distribution;
- optimizer runtime versus service count under simulation evaluation;
- placement objective improvement on a deployed cluster;
- placement exact-versus-scalable gap;
- memory use of the current rich v13 backend under a full calibration;
- global minimum replica counts for any real deployment;
- a statistically calibrated universal safety-utilization threshold.

For each of these: **NOT ESTABLISHED FROM CURRENT EVIDENCE.**

# 10. Evidence Matrix

| Claim | Why it matters | Evidence | Code/artifact reference | Strength of evidence | Remaining uncertainty |
|---|---|---|---|---|---|
| The C++ kernel makes calibration computationally feasible relative to Python | Justifies architecture and enables more replications | 35.65× median speedup with CI; direct 31.8× long-horizon run | `queue-simulator-benchmark/results/report.md`; `calibration-validation.md` | Strong for frozen benchmark workload/hardware | Current v13 is richer; end-to-end calibration overhead and other hardware are only narrowly replayed |
| Native state capture and bucketing are semantically consistent on deterministic fixtures | Warm-up analysis depends on exact state areas | Six cross-kernel fixtures; current sanitizer-focused tests pass | benchmark report; `tests/test_simulation_state_capture.py`; native tests | Strong deterministic evidence | Does not establish stochastic steady-state correctness |
| Current backend supports rich distributions and call semantics | Required for representative deployment models | Current code + passing distribution/call/queue tests | `performance_models/workload.py`; C++ `main.cpp`; 556-test run | Strong implementation evidence | Real measurement identifiability and prediction accuracy remain uncertain |
| Traffic equations can predict mean internal flow | Validates queue-network topology abstraction | STELAR maximum flow error 1.44% in E04 | analytical-sufficiency `analysis.yaml/report.md` | Moderate, one deployment/workload | Fan-out/non-HTTP dependencies and other topologies not broadly tested |
| Analytical mean latency alone is sufficient for sizing | Would avoid simulation cost | E04 and E05 both reject this claim near capacity | STELAR reports | Strong evidence **against** within tested domain | It may still work in restricted low-load/simple regimes |
| Synchronous call holding plus path residual can predict mean latency | Supports enriched simulator model | Two fresh nearby STELAR holds: 1.85%, 3.76% errors | predictive-model `predictive_validation.yaml` | Preliminary | One residual run, v7, one topology/config/load; no p95 |
| The recommended replica vector is feasible in a controlled heavy-tail deployment | Direct sizing value | [1,2,2] upper mean bound 128.284 ms <150 ms | heldout-sizing `summary.yaml` | Moderate/strong for that one case | Only five recommendation reps; not minimal; older unspecified code snapshot |
| Kubesizer sizing generalizes across deployment families | Central thesis claim | 405-run study: 6 accepted matches, 2 under, 12 rejected | remote `analysis.yaml`, manifest | Mixed; broadest evidence but provenance-limited | Only four synthetic families; 25% accepted underprovision; dirty sources |
| Current statistical workflow controls repeated looks and classes | Needed for defensible feasibility | Bonferroni tail allocation and unit tests | `simulation_evaluation.py`; `test_simulation_evaluation.py` | Strong code-level evidence | Coverage not simulation-studied; default bootstrap resolution limits class count |
| Current global warm-up is validated at post-W windows | Removes initialization bias globally | Validation requires both an early-enough cutoff and `post_warmup_check.passed`; regression test covers rejection | `calibration/validation.py:211-241`; `tests/test_warmup_validation.py` | Strong code-level evidence | Still needs a clean deployment-scale calibration run |
| Measurement cohort is not right-censored at W+M | Tail estimates require complete slow requests | Event logic and focused tests | C++ `main.cpp`; measurement-cohort tests | Strong implementation evidence | Deadline outcomes deliberately censor service completion but count as failures |
| Fresh final validation avoids optimization selection bias | Required for final SLO claim | disjoint seeds, fixed 30 reps, no early stop, tests | `calibration/seeds.py`; `replica_validation.py` | Strong code-level evidence | No retained full current workflow experiment |
| Exact replica mode proves a bounded cost optimum | Gives a real algorithmic guarantee | uniform-cost ordering and tests | `optimization/uniform_cost.py` | Strong for deterministic/conclusive evaluator | Statistical unknowns break proof; model validity is separate |
| Scalable replica mode finds near-optimal solutions | Important for large systems | No approximation bound or retained benchmark | scalable optimizer code/tests only | Weak; claim unsupported | Gap distribution and failure-to-find-feasible rate unknown |
| CLI placement is a topology-only recommendation | Defines the scope of the returned node mapping | CLI omits per-service resource requests and reports no resource-feasibility certificate | `kubesizer_cli.py`; `optimization/node_placement.py`; `tests/test_cli.py` | Strong code-level evidence | Kubernetes scheduling feasibility must be checked separately |
| Placement improves latency/resilience in practice | Justifies second optimizer | No retained deployment experiment | placement code/tests | No empirical evidence | Objective fidelity and benefit unknown |
| Runtime estimates reliably predict user wait time | Usability claim | Code and deterministic tests only | `runtime_estimation.py`, tests | Implementation-level only | Calibration across workloads/hardware not retained |
| The tool is ready for arbitrary DevOps deployments | Intended product-level contribution | Mixed deployment-family result and evidence gates | E11 plus portable workflow | Insufficient | Measurement availability, hidden bottlenecks, p95, placement, topology diversity |

# 11. Threats to validity

## 11.1 Internal validity

- **Dirty experimental source snapshots.** The largest study records uncommitted changes in both Kubesizer and the load-test repository without recording their content. A later checkout cannot exactly reproduce the analysis.
- **Recently corrected contracts lack external reruns.** Warm-up acceptance, topology-only placement validation, timeout/failure handling, and placement-aware recalibration now have deterministic tests, but no clean deployment experiment has exercised all of them together.
- **Load-generator failures.** In E11, 52 run diagnostics failed quality gates, including saturation and scheduler delay. The analysis correctly rejects affected decisions, but the high rejection rate reduces effective evidence.
- **Instrumentation boundaries.** Client, ingress, application-entry Istio time, and CPU demand have different boundaries. Subtracting aligned means is valid for means, but misalignment or counter aggregation can create residual error.
- **Residual fitting.** Some models fit low-load unexplained latency and freeze it. This is valid as calibration, not direct measurement of a causal service. One residual run cannot characterize its variability.
- **Invalid/partial run contamination.** Several directories are explicitly invalid due to expired authentication, carryover, or interruption. Retained summaries exclude named invalid runs, but experiments without an authoritative summary remain ambiguous.

## 11.2 External validity

- The real-system studies are mostly one STELAR deployment plus four purpose-built synthetic families.
- The controlled families cover useful shapes—single, fan-out, repeated visit, shared multiclass—but not databases with lock contention, asynchronous queues, autoscaling, retry storms, caches, priorities, fork-join critical paths with dependence, or changing service meshes.
- Most decisive retained studies use mean SLOs. Generalization to p95 capacity planning is not established.
- Hardware, Kubernetes version, mesh configuration, geographic client path, and application build affect results.
- A profile is valid only for its fingerprinted stationary workload and bounded configuration domain; changing traffic, routes, work, timeouts, or concurrency requires recalibration.

## 11.3 Construct validity

- **CPU demand is not wall-clock service time.** \(U/X\) measures CPU-seconds/request over a window. I/O, locks, connection pools, worker counts, database waits, and blocking calls can dominate latency without proportionate CPU.
- **Istio duration is inclusive.** It may include downstream waiting and cannot be inserted as exclusive service demand without double counting.
- **Analytical G/G/c is an approximation.** Labelling the selected family G/G/c does not make it an exact solution.
- **Mean visits do not identify call order.** Edge counters show volume, not whether siblings are sequential, parallel, conditional, or causally nested.
- **Placement network cost is a proxy.** It is not end-to-end latency and assumes proportional replica traffic.
- **Spreading is a concentration proxy.** It is not a quantitative availability/resilience probability.
- **Stability is model-relative.** \(\rho<1\) guarantees stability only for the represented queue work/capacity under stationary assumptions, not for the uninstrumented application.
- **Empirical minimum is bounded.** E11 identifies the smallest passing configuration among configurations actually tested, not a global deployment optimum.

## 11.4 Statistical conclusion validity

- Warm-up cutoff estimates were seed-sensitive. R=100 performing worse than R=30 in five meta-ensembles warns against selecting a replication count from a single small sensitivity exercise.
- Several real experiments use only one or a few replications at a load/configuration. Systematic model bias dominates the narrow within-simulation CI.
- The cluster bootstrap assumes independent replications. Common input sources, shared cluster state, cooldown carryover, or slow environmental drift can violate this.
- Bootstrap percentile intervals have not had coverage validated under the simulator's within-run dependence and heavy tails.
- Sequential Bonferroni control is conservative, increasing inconclusive decisions and cost. Default 5,000 draws can be too coarse for many request classes.
- E11's 75% accepted-case match is descriptive, not an inferential population estimate: cases were designed, not randomly sampled.
- P95 requires many tail observations and stable tail behavior. The current external experiments mainly report mean latency.

# 12. Current thesis contribution, stated conservatively

## 12.1 Modelling contribution

The repository implements a unified request-class workload representation that can use routing probabilities, visit ratios, workflows, or synchronous call graphs; general interarrival/service distributions; class-specific service demands; network and residual latency components; CPU/concurrency execution limits; and telemetry-derived uncertainty. It connects this representation to analytical mean-flow and queueing approximations.

The defensible contribution is the integration and explicit separation of measured quantities, derived quantities, and assumptions. It is not a new exact queueing theorem, and evidence shows that mean CPU-demand queueing alone is insufficient near some real capacity boundaries.

## 12.2 Simulation contribution

The work includes a C++20 event kernel designed for calibration-scale replication, with external request cohorts, drain semantics, synchronous call holding, multiple queue/execution models, distribution plugins, placement delays, timeouts, failures, and exact event-driven state integration. A Python layer performs statistical analysis and orchestration.

The performance contribution is well supported for the frozen kernel benchmark. Predictive accuracy of the current rich semantics is only partly supported and not established generally.

## 12.3 Replica-sizing contribution

The system combines an analytical stability lower bound, optional telemetry-derived construction seed, exact bounded uniform-cost search, and a scalable randomized greedy/deletion/VNS heuristic. Simulation mode requires confidence-bound-certified feasibility and preserves inconclusive outcomes. Fresh final validation is architecturally separated from optimization.

This is a meaningful engineering/research prototype. The current evidence does not support a claim that it produces reliable initial sizing for arbitrary deployments: two target families were underprovisioned in the largest study.

## 12.4 Placement contribution

The repository has exact branch-and-bound and scalable diversity-aware beam/VNS/LNS solvers for a latency-weighted communication plus replica-spreading objective. The algorithms expose honest local/global metadata.

The active CLI resource-constraint gap and absence of placement experiments prevent a strong applied contribution claim today.

## 12.5 Experimental contribution

The retained work includes:

- a controlled cross-language simulator benchmark;
- direct long-horizon calibration timings;
- negative analytical-model sufficiency studies;
- a preliminary residual/synchronous-call holdout;
- a controlled heavy-tail sizing holdout;
- a 405-run, four-family Kubernetes study with quality gates and 30-replication target validation.

The most valuable scientific outcome is that negative findings were retained: accurate mean flow does not imply accurate latency, and even an enriched simulator can underprovision simple/fan-out cases. This directs the next thesis phase toward model-identifiability and external validation rather than further optimizer complexity.

# 13. What remains before the thesis is defensible

## P0 — necessary for correctness

1. **Create a clean, immutable experiment provenance bundle.** Commit/tag exact Kubesizer and load-test sources before experiments; record container images, manifests, input hashes, cluster snapshot, analysis command, and output checksums. The largest existing study cannot be exactly reproduced from Git.
2. **Run one clean end-to-end current-revision experiment.** Produce and retain the actual profile, \(W\), \(M\), optimizer evaluation history, placement-aware recalibration, final fresh validation, and final recommendation under v13. Code-level tests and retained-data replay do not establish real-system predictive validity.
3. **Resolve default bootstrap resolution for many classes.** Calculate bootstrap draws from classes/looks/tail-resolution or use a suitable spending/interval method. Defaults should not fail when the intended workload has around 20 request classes.

## P1 — necessary for a strong thesis

1. **Repeat deployment-family validation after model fixes, on a clean revision.** Keep the same four families for paired comparability, correct the single/fan-out underprovisioning, and add fresh seeds. The principal success criterion should be no accepted unsafe recommendation, with conservative overprovision reported separately.
2. **Validate p95, not only mean.** Run enough independent real replications and eligible requests for at least one p95 SLO per topology. Compare pooled observed p95 with the simulation's cluster-bootstrap interval and validate final SLO decisions.
3. **Run exact-versus-scalable optimizer experiments.** Generate small instances where exact optimum is tractable, then report optimum-hit rate, median/p95 gap, failure-to-find-feasible rate, evaluations, and runtime over many fixed seeds. Repeat on simulation-backed fixtures where affordable.
4. **Evaluate placement scientifically.** First enforce resources. Then compare branch-and-bound and Beam/VNS objective gaps on small cases and deploy selected placements to measure actual end-to-end latency and traffic locality. Include a no-placement/random/spread baseline.
5. **Perform model ablation.** On held-out families compare: analytical only; basic queue simulator; +synchronous holding; +measured concurrency; +communication; +latency components; +joint input bootstrap. This identifies which complexity creates predictive value.
6. **Characterize the residual with multiple independent low-load runs.** Store mean, variance/distribution, and boundary semantics. Verify whether it remains invariant across load and replica changes; if not, do not model it as fixed overhead.
7. **Measure identifiable capacity boundaries.** For key services, retain healthy and failing runs at known replica counts. Use these as hard evidence/risk gates rather than inferring saturation solely from aggregate CPU demand.

## P2 — useful enhancement

1. Make measurement-duration prefix reuse fully efficient by running the longest planned trajectory once per seed/configuration.
2. Align warm-up horizon slack with processing slots and class-specific service mixtures.
3. Add persistent, fingerprint-keyed profile and replication caching as an opt-in library feature, with explicit invalidation.
4. Add topology-aware affinity, anti-affinity, zones, node selectors, and pod overhead to placement.
5. Empirically calibrate runtime estimates across service count, arrival volume, distribution tails, \(W\), \(M\), and optimizer mode.
6. Retain generated calibration and sequential-CI plots automatically in an experiment bundle, while keeping normal CLI output concise.

## P3 — optional or future work

1. More efficient alpha-spending or anytime-valid confidence sequences to reduce conservative Bonferroni cost.
2. More realistic state-dependent retries, caches, autoscaling, asynchronous brokers, and fork-join dependence where measurements justify them.
3. Alternative simulator bindings that avoid subprocess serialization if profiling shows it matters.
4. Robust optimization over explicitly weighted workload regimes; this should follow, not precede, reliable single-regime validation.
5. A calibrated availability objective for placement using failure-domain data instead of a generic spreading proxy.

# 14. 5-Minute Supervisor Briefing

### Problem

The thesis addresses initial Kubernetes microservice capacity planning: given a workload, measured service behavior, request paths, SLOs, and a cluster, recommend replica counts and optionally place those replicas on nodes. The difficult part is not only combinatorial search. It is deciding whether a candidate is genuinely stable and likely to satisfy end-to-end latency when real services have variable work, synchronous dependencies, hidden bottlenecks, and noisy measurements.

### Method

The current method has three levels. First, it solves per-request-class traffic equations and queueing approximations to estimate service visit rates, utilization, strict mean-work stability, and mean latency. Second, when simulation is selected, a C++ discrete-event model represents request classes, queues, processing/concurrency capacity, distributions, synchronous calls, network delay, timeouts, and measured latency components. Python calibrates warm-up and measurement duration and uses independent replications with cluster-bootstrap confidence intervals. Third, exact or scalable optimizers search replica vectors and node placements. Simulation-based feasibility requires every class's confidence upper bound to pass; unresolved candidates are not accepted.

### What is implemented

- native and portable DevOps evidence inputs;
- multiclass traffic propagation with routing, visits, workflows, and calls;
- M/M/c and M/G/1 exact means plus variability-adjusted multiserver approximations;
- strict analytical stability and minimum stable replica vector;
- mandatory C++20 event simulator with rich distributions and event-driven state capture;
- Welch-style ensemble warm-up analysis, measurement-cohort draining, measurement-duration calibration, disjoint seed namespaces, sequential cluster-bootstrap evaluation, and fresh final validation;
- exact uniform-cost and scalable randomized greedy/deletion/VNS replica sizing;
- exact branch-and-bound and scalable beam/VNS/LNS node placement;
- evidence-quality gates and runtime estimation.

The current test suite passes 556 tests. Release and sanitizer native builds pass, and 74 focused simulation/native tests pass under sanitizers.

### What has been experimentally evaluated

There are five retained verified experiment families: a cross-language kernel benchmark; direct long-horizon calibration timing; an analytical sufficiency study; an operation-aware analytical holdout; and a controlled heavy-tail sizing holdout. The 405-run family study, warm-up sensitivity study, synchronous-call holdout, and current-v13 replay are preliminary under the current evidence-retention standard.

There are no retained scientific experiments for current placement quality, exact-versus-scalable optimizer gaps, current v13 p95 prediction, or a clean live full current-revision calibration-to-final-validation run.

### Most important results so far

- C++ was 35.65× faster than custom Python in the key 30-replication capture benchmark, with 95% bootstrap interval [34.82×, 36.20×]. A direct long-horizon run was 31.8× faster.
- STELAR mean-flow prediction had maximum 1.44% error in one study, but near-capacity analytical latency predicted 330.658 ms while the live system measured 1538.487 ms. Correct flow did not imply correct capacity.
- A narrow synchronous-call simulation holdout had 1.85% and 3.76% mean-latency error in two fresh nearby runs, but used one residual-calibration run and older v7 semantics.
- In a controlled heavy-tail system, recommended [1,2,2] was feasible: 116.706 ms mean with 95% t interval [105.129,128.284] under a 150 ms SLO. A cheaper neighbor also passed the pooled mean, so the recommendation was conservative rather than minimal.
- In the 405-run family study, 12 of 20 decisions were evidence-rejected. Of eight accepted decisions, six matched the tested empirical minimum and two were underprovisioned. At 30-replication target validation, repeated and shared families passed; single and fan-out failed.
- Replaying current v13 on the four retained 30-run target points gave the safest portable model 3/4 decision agreement, zero unsafe false-feasible decisions, one conservative false-infeasible decision, and 28.1% mean absolute percentage error. A full single-family workflow selected five replicas; 30 fresh simulation seeds gave 90.006 ms with simultaneous 95% bounds [89.376, 90.666] under a 100 ms mean SLO. No retained live five-replica run exists.

### What the results indicate

The C++ architecture successfully removes simulation runtime as the dominant obstacle. The modelling/measurement problem is now more important than language performance. Traffic equations are useful for propagation and screening. Rich simulation can improve prediction, but current measurement-derived semantics still omit or misattribute enough latency/capacity behavior to make unsafe recommendations in some simple topologies.

The tool is therefore a strong prototype and experiment platform, not yet a generally trustworthy automatic sizer.

### Current limitations

Most real evidence concerns mean latency rather than p95. The largest study used dirty, unreconstructable source snapshots. Aggregate CPU/throughput data do not identify wall-clock service-time variance or non-CPU capacity. Placement and scalable-search quality have not been experimentally assessed. The newly tightened warm-up, terminal-failure, and placement-aware validation paths are code-tested but have not yet been rerun as a clean deployment experiment.

### Next experiments

1. Run one clean end-to-end experiment with the current implementation, retaining the full profile and placement-aware final validation.
2. Repeat the four-family study with paired workloads and fresh seeds, focusing on eliminating unsafe single/fan-out decisions and adding p95 SLOs.
3. Run exact-versus-scalable replica and placement benchmarks, then deploy selected placements against random/spread baselines.

# Appendix A. Verification performed during this audit

The following inexpensive checks were executed against the audited working tree:

| Check | Command | Result |
|---|---|---|
| Full native build and Python suite | `make test` | **556 passed in 8.80 s** |
| C++ Release build | `./native_backend/cpp/build.sh` | passed |
| C++ sanitizer build | `./native_backend/cpp/build.sh --sanitizers` | passed |
| Focused sanitizer-backed native/statistical tests | sanitizer backend via `KUBESIZER_CPP_BACKEND=...` with native, consistency, state, workflow, call, concurrency, and distribution tests | **74 passed in 16.76 s** |
| Static typing | `mypy` over 61 source files | passed with no issues; untyped PyYAML imports are explicitly isolated |
| Formatter/linter availability | configured command inspection | no repository-wide configured lint target found; `ruff` executable was not available through the checked command path |
| Analytical CLI smoke | minimum workload, analytical exact, skip placement, quiet | completed; one recommended replica for the single service |
| Current retained target replay | four deployment families, five model variants, 30 replications, 20,000 bootstrap draws | safest portable model: 3/4 decisions, zero unsafe false-feasible, 28.1% MAPE |
| Current public workflow replay | characterized single-service target, exact search through five replicas, 30 fresh validation runs | \(W=87.918\) s, \(M=1536\) s, selected five replicas; final mean 90.006 ms [89.376, 90.666] under 100 ms |

Passing tests establish implementation consistency with the tests. They do not establish external predictive validity.

# Appendix B. Important code map

| Concern | Primary code |
|---|---|
| CLI orchestration | `kubesizer_cli.py` |
| Public workflow | `statistical_workflow.py` |
| Runtime advisory | `runtime_estimation.py` |
| Workload schema/distributions | `performance_models/workload.py` |
| System construction, traffic, analytics, simulation adapter | `performance_models/system.py` |
| Queue formulas | `performance_models/analytical.py` |
| Native Python adapter | `native_backend/backend.py` |
| C++ kernel | `native_backend/cpp/src/main.cpp` |
| State collector | `metrics/simulation_metrics_collector.py` |
| Exact bucketizer | `metrics/time_weighted_buckets.py` |
| Cluster bootstrap | `metrics/cluster_bootstrap.py` |
| Warm-up calibration | `calibration/cutoff.py`, `ensemble.py`, `sensitivity.py`, `validation.py`, `workflow.py` |
| Measurement calibration | `calibration/measurement.py` |
| Profile/fingerprints/seeds | `calibration/profile.py`, `seeds.py` |
| Simulation candidate evaluation | `optimization/simulation_evaluation.py` |
| Fresh final validation | `optimization/replica_validation.py` |
| Replica problems | `optimization/replica_sizing.py` |
| Exact replica search | `optimization/uniform_cost.py` |
| Scalable replica search | `optimization/scalable_replica.py` |
| Placement problem/objective | `optimization/node_placement.py` |
| Exact placement | `optimization/branch_and_bound_node_placement.py` |
| Scalable placement | `optimization/scalable_node_placement.py` |
| Mode selection | `optimization/mode.py`, `optimization/selection.py` |
| Portable evidence model | `characterization/models.py`, `parser.py`, `analysis.py` |
| Evidence seed and readiness | `characterization/sizing.py`, `readiness.py`, `workflow.py` |
| Measured-semantics simulation builder | `characterization/simulation.py`, `latency.py` |

# Appendix C. Bottom-line claims

The following claims are presently defensible:

- A compiled C++ event kernel can make repeated warm-up simulations tens of times faster than the Python alternatives on the retained benchmark.
- The current source implements a statistically cautious simulation-evaluation architecture that distinguishes feasible, infeasible, inconclusive, and failed results.
- The active CLI can recommend a network/spreading placement without requiring pod requests or limits, but it does not claim Kubernetes resource feasibility.
- Mean traffic equations can be accurate even when latency/capacity predictions are wrong.
- Enriching the simulator with synchronous call holding, distributions, measured latency layers, and input uncertainty is technically implemented and has encouraging narrow evidence.
- Some controlled replica recommendations are feasible, but the method can also underprovision.

The following claims are not presently defensible:

- Kubesizer reliably sizes arbitrary Kubernetes microservice deployments.
- The current v13 simulator still has material and topology-dependent prediction error.
- The scalable optimizers are near-optimal.
- The placement objective improves deployed latency or availability.
- The current calibration workflow has been externally validated end to end.

The thesis is moving in a technically productive direction, but its strongest next contribution should come from correcting the identified contracts and executing clean, paired validation experiments—not from adding more model or optimizer features before the existing recommendation boundary is quantified.
