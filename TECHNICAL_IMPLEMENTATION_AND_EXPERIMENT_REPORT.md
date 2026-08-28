# Kubesizer: implementation, testing, and experiment handoff

Date: 28 August 2026

This report explains, in simple language, what Kubesizer is trying to do, why the first versions were not reliable enough, what was changed, what was tested, and what is still running. It is also a handoff document for continuing the work from another computer.

## 1. The goal

Kubesizer is meant to answer a practical question:

> For this measured workload, how many replicas should each service start with?

The recommendation does not have to be the unique perfect configuration. It should be a sensible and defensible initial size that a DevOps engineer can use before normal production tuning takes over.

The intended flow is:

1. A DevOps engineer measures one representative workload.
2. The measurements describe arrival rates, service demand, request paths, variability, latency boundaries, and resource limits when these are available.
3. Kubesizer checks whether the evidence is good enough to use.
4. Kubesizer builds an analytical or simulation model from that evidence.
5. The replica optimizer searches for a configuration that meets the declared SLOs.
6. When simulation is used, fresh independent runs validate the selected configuration inside the declared model.
7. Real-cluster experiments are used separately to determine whether the model represents the deployment well enough.

The words “inside the declared model” matter. No simulator can guarantee a real deployment if an important bottleneck was never measured or modeled.

## 2. What the first version did

The first version used a classic open queueing network:

- external arrivals were exponential;
- service times were exponential;
- each service behaved like an M/M/c queue;
- requests followed fixed routing probabilities;
- replica count was the queue capacity;
- the traffic equations calculated the effective rate at each service;
- the optimizer started at the minimum analytically stable replica vector;
- an A* search added replicas until it found a configuration that met the evaluator's SLO test.

This was a useful prototype. It proved that the workload could be represented as a graph and that replica sizing could be expressed as a search problem.

It was not yet a reliable general sizing tool.

## 3. Why the old approach was not enough

There were two different problems.

### 3.1 The search problem

A* needs a heuristic that predicts how far a state is from the goal. The old heuristic mainly used analytical stability. Stability only says that, in the ideal mathematical queue, work does not accumulate forever on average:

```text
arrival rate < total service capacity
```

That is a necessary lower bound, but it is not an SLO prediction. A mathematically stable queue can still have unacceptable latency, especially when utilization is close to 100%.

Because the heuristic did not closely predict the true SLO objective, A* offered little useful guidance. The bounded configuration space also grows as the product of every service's replica choices. Even a good exact search becomes expensive when many services have several possible replica counts.

The search algorithms were therefore changed:

- exact replica sizing now uses uniform-cost search, which makes its exact behavior honest and easy to reason about;
- scalable replica sizing uses randomized greedy construction, deletion descent, multiple starts, and variable-neighborhood search;
- exact node placement uses branch-and-bound;
- scalable node placement uses diversity-aware beam search, relocation, swaps, and a small large-neighborhood search;
- `AUTO` chooses between exact and scalable search from a conservative estimate of the actual bounded search space;
- replica sizing and node placement can be selected independently, so a hybrid run can use exact search for one and scalable search for the other.

The scalable algorithms are heuristics. They can return a certified feasible configuration, and sometimes a local-minimality certificate, but they do not claim a global optimum without a valid lower bound.

### 3.2 The model problem

The optimizer can only be as good as its evaluator. A fast search over an inaccurate latency model still produces a poor recommendation.

The original M/M/c model missed several common Kubernetes effects:

- service times are often not exponential;
- traffic can be bursty rather than Poisson;
- a request may call several downstream services;
- it may call the same service more than once;
- downstream calls may run sequentially or in parallel;
- an upstream worker may remain occupied while waiting for downstream work;
- pods usually have separate queues rather than one perfect shared queue;
- CPU demand does not reveal thread pools, connection pools, locks, disk waits, database waits, or socket limits;
- network, ingress, serialization, and client-side delay are outside the application CPU queues;
- failures and timeouts are part of the user-visible result and cannot simply be discarded.

The experiments confirmed this limitation. The original analytical model predicted service flow well, but it badly underestimated latency near capacity. At the highest tested STELAR load, its application-entry prediction was about 331 ms while the observed mean was about 1,538 ms. The predicted visit rates were still close to the observed rates. This showed that routing was not the main problem; unmodeled variability and non-CPU concurrency were.

This is why “the old optimizer failed to model the system” is not quite the right description. The optimizer searched the model it was given. The main failure was that the evaluator's system model was too simple.

## 4. Why C++ was chosen

Statistically controlled simulation needs many independent runs. Warm-up studies may need several horizons, several bucket widths, and tens of replications. Each candidate configuration may then need more replications for a confidence interval. SimPy creates Python generators and events for requests and resources. That design is excellent for clarity and flexibility, but the Python overhead became large for this workload.

The warm-up calibration exposed the problem clearly. Pilot horizons grew from 500 to 1,000, 2,000, 4,000, 8,000, and 16,000 simulated seconds without passing the horizon-sensitivity rule. More CPU cores helped independent replications, but they did not remove the single-run Python event overhead.

A separate benchmark repository was used to compare the SimPy behavior with a custom event kernel. The important architectural change was not just “use another language.” It was:

- one central future-event heap;
- compact request and service state;
- no Python coroutine for every request;
- no thread for every request;
- events ordered deterministically by time, priority, and sequence number;
- independent replications parallelized with a bounded worker pool.

C++ was selected as the required simulation executor because it gave this compact event-kernel design low overhead and predictable memory use. Python still owns the parts that are easier to inspect and change:

- workload parsing;
- analytical calculations;
- statistical design;
- calibration decisions;
- optimization;
- confidence intervals;
- final classification;
- reporting.

The C++ program is therefore a simulation engine, not a complete replacement for Kubesizer. Python sends it a workload, replica configuration, seeds, warm-up duration, and measurement duration. C++ returns latency samples, counts, state information, and diagnostics.

There is no SimPy fallback in the current production path. The native backend is required. This avoids silently producing different results depending on which backend happened to be installed.

### 4.1 The benchmark evidence

The language decision was made from a separate benchmark repository, not from
guesswork. Five implementations were compared against frozen simulation
semantics:

- the SimPy baseline;
- a custom Python event kernel;
- Java;
- Go;
- C++.

All five passed the deterministic semantic fixtures. SimPy and custom Python
also matched under shared stochastic seeds. The compiled implementations used
their normal language random generators, so their stochastic validation checked
fixed-seed reproducibility, state invariants, and agreement of distributions
across 30 replications rather than demanding identical random paths.

For the representative calibration workload—30 state-capturing replications on
eight workers—the median times were:

| Implementation | Median time |
|---|---:|
| SimPy | 4.683 seconds |
| custom Python | 2.468 seconds |
| Java | 0.302 seconds |
| Go | 0.321 seconds |
| C++ | 0.069 seconds |

C++ was about 35.6 times faster than the custom Python kernel in this key mode.
The bootstrap 95% interval for that speedup was about 34.8 to 36.2 times. It was
also about 4.4 times faster than Java and 4.6 times faster than Go in the same
comparison.

A direct long-horizon follow-up ran 30 replications to simulated time 16,000
with exact online time-weighted buckets. Custom Python took about 60.8 seconds;
C++ took about 1.9 seconds, a measured speedup of about 31.8 times. With 300
replications at horizon 32,000, three independent C++ ensembles finished in a
median of about 26.2 seconds.

Memory also supported the decision. In the thesis-like parallel state-capture
case, median process-tree peak memory was about:

| Implementation | Peak resident memory |
|---|---:|
| SimPy | 512 MiB |
| custom Python | 434 MiB |
| Java | 411 MiB |
| Go | 119 MiB |
| C++ | 44 MiB |

These numbers are benchmark results for the tested machine and workloads, not
universal language guarantees. They do show that C++ made the expensive
calibration design computationally realistic. The benchmark also found that
custom Python was already fast enough for many short runs. C++ was selected
because the production plan included long calibration ensembles, repeated
sensitivity checks, and many optimizer evaluations.

The benchmark found an important statistical issue too: faster execution does
not guarantee a stable warm-up detector. Independent 30- and 100-replication
ensembles could choose different earliest acceptable windows. At 300
replications, the tested horizon-doubling checks became stable. This is why the
current architecture keeps statistical decisions in Python and treats the C++
kernel as a fast source of evidence rather than as the authority that declares
convergence.

## 5. What the C++ simulator now models

The native backend supports the behavior needed by the current sizing workflow:

- several request classes running at the same time;
- a mandatory concurrency weight for each request class;
- multiple external entry services;
- exponential, deterministic, gamma, lognormal, Weibull, bounded-uniform, empirical-CSV, and trusted native custom distributions where supported by the workload parser;
- fixed replica counts for one simulation candidate;
- fixed service slots and processor-sharing execution models;
- pooled queues or independent per-replica queues;
- round-robin or random dispatch to independent replica queues;
- FIFO waiting;
- probabilistic routing and service revisits;
- explicit request call graphs;
- sequential or parallel downstream calls;
- synchronous parent concurrency held while downstream work completes;
- request-class deadlines and timeout outcomes;
- client-to-ingress, ingress-processing, same-node, cross-node, and residual latency components;
- placement-aware directional network latency;
- event-driven active-request, queue, and busy-replica state;
- fixed-horizon runs;
- arrival-cohort measurement with draining;
- independent replications with bounded native parallelism.

The fundamental state histories are:

- `A(t)`: active end-to-end requests;
- `Q_i(t)`: requests waiting at service `i`;
- `B_i(t)`: busy replicas or slots at service `i`.

Number in system and utilization are derived from those values. They are not maintained as competing counters.

## 6. Workload input

The current workload has one `traffic_model`. If the engineer wants to evaluate another operating condition, they change the workload and run Kubesizer again. This removed the confusing old scenario/override structure.

Each request class must have a `concurrency_weight`. The weights sum to one. They describe the traffic mixture; they do not reserve capacity or create queue priority.

The workload can contain:

- external arrival distributions and rates by class and entry service;
- service-time or effective-demand distributions;
- per-service execution and queue-dispatch semantics;
- routing or explicit call graphs;
- latency boundary and measurable non-service components;
- mean SLOs;
- percentile SLOs.

A percentile SLO requires simulation because the strict analytical path cannot honestly derive a general p95 for all supported distributions and call semantics. A mean SLO can be evaluated analytically or by simulation.

## 7. Analytical methods and simulation

The analytical layer was generalized beyond M/M/c. It uses the available distribution moments and selects a compatible approximation, including variability-adjusted G/G/c behavior when appropriate.

The analytical and simulation paths have different jobs:

- analytical equations are fast and useful for traffic-flow checks, stability lower bounds, candidate ranking, and screening clearly underprovisioned configurations;
- simulation represents richer request paths, full input distributions, queue dispatch, blocking, deadlines, and network components;
- real-cluster holdouts test whether either model matches the actual deployment.

Analytical and simulation modes do not vote together. The chosen evaluator determines SLO feasibility. Analytical stability remains a required precheck before an expensive simulation.

## 8. Statistical simulation workflow

The statistically controlled path has three separate stages.

### Stage A: calibration

Calibration freezes the experiment design for a topology and workload:

- global warm-up duration `W`;
- measurement-arrival-window duration `M`;
- minimum and maximum replication counts;
- replication batch size;
- sequential-look count;
- confidence level;
- bootstrap settings;
- sample requirements;
- disjoint seed namespaces.

Warm-up is based on actual state, not on a final latency number. Empty-start replications record active requests, queues, and busy capacity. Exact time-weighted buckets are calculated by integrating the state between events. The detector uses the late part of the run as a reference, requires several stable windows, checks their trend, adds a safety window, and validates horizon and bucket-width sensitivity. If an important state never settles, calibration fails instead of inventing a number.

Measurement duration solves a different problem. It controls how much p95 data is collected. Candidate durations are doubled until every required class has enough samples and the p95 interval is sufficiently precise and stable.

### Stage B: configuration evaluation

Every stable candidate starts independent empty-state replications. The system runs continuously through warm-up; no queue, request, resource, clock, or random stream is reset at `W`.

The measured cohort is defined by arrival time:

```text
W <= request arrival < W + M
```

Eligible requests are followed until completion even if they finish after `W + M`. This prevents slow requests from disappearing from the p95 sample. Background arrivals can continue so the queue is not artificially drained at the measurement boundary.

For each request class, the point p95 is calculated from the pooled eligible requests. Confidence intervals use a replication-cluster bootstrap: complete replication latency arrays are resampled, not individual requests. This preserves dependence among requests that shared the same run.

Repeated looks and several request classes use conservative Bonferroni error allocation. A candidate is:

- feasible only when every upper confidence bound meets its SLO;
- infeasible when at least one lower bound is worse than its SLO;
- sampled again when the interval overlaps the SLO and budget remains;
- inconclusive when it still overlaps at the maximum replication count.

`INCONCLUSIVE` is not silently called feasible or infeasible.

### Stage C: final validation

After optimization selects a candidate, that candidate is rerun with a fresh seed namespace. Optimization samples are not reused. Final validation uses a fixed replication count and does not stop early. The result passes only when every fresh upper bound meets the SLO.

This proves feasibility inside the frozen model. It does not prove that the model contains every real-cluster bottleneck.

## 9. Evidence gate and safe failure

Monte Carlo intervals measure uncertainty inside the simulator. They cannot detect a missing database, thread pool, or network effect. Kubesizer therefore has a predictive-validation gate and a portable evidence-readiness gate.

The evidence checks include:

- measurement identity and units;
- service and request-class coverage;
- observed load range;
- CPU-demand stability and load correlation;
- capacity-run coverage;
- censoring and client saturation;
- service-flow agreement;
- model fingerprint compatibility;
- fresh holdout prediction error.

If the input is not strong enough, the public recommendation path reports failure instead of fabricating a replica vector. Lower-level diagnostic functions still expose the partial calculations so the engineer can see what measurement is missing.

Censored requests receive special treatment. Their elapsed time is a lower bound. They may help prove that a configuration is too slow, but a run containing unresolved censored requests cannot prove feasibility.

## 10. Load generation and measurement repository

The load and measurement work lives in a separate repository:

```text
/home/ktistos/work/STELAR_LOAD_TESTS
```

It uses Locust for controlled open-loop load and standard cluster telemetry. It does not require changes to application source code.

It measures or derives:

- aggregate external arrival rate at known entry workloads;
- per-service incoming rate;
- source-to-destination edge call rates;
- routing probabilities only where single-next-hop semantics are valid;
- visit ratios, which may exceed one because of fan-out or repeated calls;
- ingress and per-service Istio mean and percentile latency;
- per-pod application CPU usage, excluding `POD` and `istio-proxy`;
- per-pod throughput using the namespace-and-pod join;
- CPU demand per request as `CPU usage / throughput`;
- CPU-based capacity estimates when a defensible CPU capacity is available;
- CPU and memory requests and limits;
- 2xx, 4xx, and 5xx error rates;
- raw PromQL, raw samples, capability discovery, and limitations.

It keeps important meanings separate. CPU-seconds per request is not called wall-clock service time. Istio response time is inclusive and is not called intrinsic service demand.

The collector first inspects the labels that actually exist. It does not globally treat `source_workload="unknown"` as user traffic. Aggregate external traffic is attributed only when an unknown source reaches an explicitly configured or safely discovered entry workload. Possible health-check and probe contamination is recorded.

Request-class metrics remain unavailable unless a bounded operation label such as `request_operation` exists. Controlled one-class runs can still attribute the generated cohort because the experiment itself produced only that class.

## 11. How the load tests avoid misleading results

The load generator supports exponential, deterministic, gamma, lognormal, Weibull, bounded-uniform, and empirical interarrival distributions. Each class is an independent open-loop renewal stream, so requests can overlap.

The tests include:

- a warm-up period without resetting the application or random stream;
- a measurement arrival window;
- a drain period for eligible requests;
- request-level scheduled, actual-start, and completion timestamps;
- scheduler-delay diagnostics;
- a configurable in-flight safety limit;
- an HTTP connection pool sized consistently with the in-flight limit;
- cooldown between high-load runs;
- reproducible independent seeds;
- saved configuration and cluster context;
- restoration of the original deployment after success, failure, or interruption.

HTTP failures are retained. Authentication failures and invalid request definitions are separated from capacity failures. A 401 or 403 is not evidence that the service needs more replicas.

## 12. What the completed experiments showed

### 12.1 STELAR analytical sufficiency study

The conclusion was `NOT_SUFFICIENT_WITHIN_TESTED_DOMAIN`.

The model predicted service flows accurately, with a maximum service-flow error around 1.44%, but latency error became large near capacity. At roughly 56 requests/second:

- predicted client mean was about 399 ms;
- observed client mean was about 1,577 ms;
- observed client p95 was about 2,924 ms;
- all three near-capacity replications violated the mean SLO or had failures.

The experiment also showed why adding a fixed latency overhead is not enough. A fixed residual can represent roughly constant ingress, network, or baseline application delay at low load. It cannot represent a load-dependent queue, worker limit, connection pool, or hidden dependency.

### 12.2 Operation-aware study

Operations were measured separately, their visit patterns and CPU-demand contributions were compared, and similar operations were clustered. The enriched analytical model still failed as the only mean-SLO oracle near the difficult load point.

This did not make clustering useless. Clustering remains useful for reducing many endpoints to a smaller set of representative request classes. It just needs a fresh mixed-workload holdout before its latency prediction is trusted.

### 12.3 Heavy-tail synthetic sizing holdout

A controlled three-service deployment used exponential arrivals and lognormal service times with SCV 4.0. This experiment tested richer simulation behavior.

The recommended configuration was:

```text
front:   1
worker:  2
backend: 2
```

Across five fresh replications it had:

- 21,318 eligible requests;
- zero client failures;
- observed mean latency about 116.7 ms;
- a 95% interval over replication means of about 105.1 to 128.3 ms;
- a frozen simulation prediction around 134.9 ms;
- a 150 ms mean SLO.

The cheaper one-backend neighbor had an acceptable overall mean in 20 runs, but four finite windows exceeded the SLO and one request timed out. It was therefore judged to have insufficient operational margin. A one-worker neighbor was decisively infeasible, with mean latency over one second.

This showed that the enhanced simulator could identify the worker bottleneck and return a safe initial configuration in that controlled family. It did not establish a universal guarantee.

## 13. Current deployment-family experiment

A larger experiment is running independently on a Hetzner VM. It is designed to test several deployment shapes rather than one topology:

- single service;
- fan-out;
- repeated visits;
- shared multi-class services;
- low, target, high, and bursty traffic points;
- lower, candidate, and safety replica vectors;
- fresh target-validation replications.

The plan contains 405 load-test runs. Normal points use exponential arrivals. Bursty points use lognormal interarrivals with SCV 4. Configurations in the same block use common random-number seeds; independent blocks use disjoint seeds. The final target validation uses 30 fresh replications.

VM details:

```text
provider: Hetzner Cloud
region: Nuremberg
machine: CCX13
CPU: 2 dedicated AMD EPYC Milan vCPUs
RAM: about 7.6 GiB
OS: Ubuntu 24.04.4 LTS
public address: 46.224.93.195
SSH user: experiment
SSH key on this computer: ~/.ssh/hetzner-loadtest
```

The application cluster is in Athens. The VM owns the load process, Prometheus tunnel, logs, and study output. The experiment does not depend on this development computer remaining powered on.

At the handoff snapshot, the system service was active, 94 of 405 runs were complete, and run 95 (`single__high__c1__rep-02`) was active. The latest logged estimate was about 25 hours and 25 minutes remaining. This estimate changes with rollout and request drain time.

The system service is:

```text
kubesizer-sizing-study.service
```

Important remote paths:

```text
/home/experiment/experiments/deployment-family-sizing-validation-20260827
/home/experiment/experiments/deployment-family-sizing-validation-20260827/study_manifest.yaml
/home/experiment/experiment-logs/kubesizer-sizing-study.log
/home/experiment/work/STELAR_LOAD_TESTS
/home/experiment/work/Kubesizer
```

Check it from another computer with the private SSH key:

```bash
ssh -i ~/.ssh/hetzner-loadtest experiment@46.224.93.195 \
  'systemctl status kubesizer-sizing-study.service --no-pager -l'

ssh -i ~/.ssh/hetzner-loadtest experiment@46.224.93.195 \
  'tail -n 30 /home/experiment/experiment-logs/kubesizer-sizing-study.log'
```

One transient Prometheus port-forward failure occurred after 93 completed runs. Completed results were preserved. The runner was changed to retry port-forward startup five times with increasing delay, archive an incomplete attempt, clear stale failure state when resuming, and continue from completed runs. Two interrupted run directories were archived during recovery. The service is configured to restart on failure.

After the study finishes, analyze it on the VM with:

```bash
cd /home/experiment/work/STELAR_LOAD_TESTS
.venv/bin/python scripts/analyze_sizing_study.py \
  --plan synthetic/deployment_families/sizing-study.yaml \
  --study-dir /home/experiment/experiments/deployment-family-sizing-validation-20260827 \
  --kubesizer-root /home/experiment/work/Kubesizer \
  --output /home/experiment/experiments/deployment-family-sizing-validation-20260827/analysis.yaml
```

The experiment's temporary Kubernetes RBAC, namespaces, and credential should be removed after results and restoration are verified. The credential and SSH private key are deliberately not stored in Git.

## 14. Tests run so far

The following full suites passed before this handoff commit:

```text
Kubesizer:         516 tests passed
STELAR_LOAD_TESTS: 110 tests passed
simulator benchmark: 23 Python tests passed
```

The full suites also passed on the experiment VM during setup (516 Kubesizer
tests and the then-current 109 load-test tests). Focused recovery tests for the
resumable study runner passed after the port-forward retry change. The extra
local test in the final count covers the new recovery behavior.

The benchmark's shared validator also passed all six deterministic fixtures
across SimPy, custom Python, Java, Go, and C++. Its stochastic validation passed
all seven workloads with 30 replications per compiled implementation.

The test coverage includes:

- analytical traffic equations and stability;
- workload parsing and fingerprints;
- supported stochastic and empirical distributions;
- request-class weighting;
- native backend protocol and distribution behavior;
- replica queue dispatch;
- service concurrency limits;
- warm-up histories and exact bucket areas;
- measurement cohort boundaries and draining;
- bootstrap reproducibility and sequential classification;
- exact and scalable replica optimization;
- exact and scalable node placement;
- runtime estimates and progress events;
- predictive validation and recommendation refusal;
- measurement capability discovery;
- Prometheus calculations and pod joins;
- operation profiles and clustering;
- analytical and simulation validation;
- resumable, randomized sizing-study plans;
- restoration and interrupted-run handling.

These suites were rerun against the exact source trees prepared for the handoff
commit. The final terminal response should be treated as authoritative if a
later verification run differs from this snapshot.

## 15. Files and repositories needed on another computer

Kubesizer repository:

```text
GitHub: https://github.com/Ktistos/kubesizer.git
branch: feature/statistical-simulation-workflow
```

Load-test repository:

```text
GitHub repository: Ktistos/STELAR_LOAD_TESTS
branch: main
visibility: private unless explicitly changed later
```

Simulator benchmark repository:

```text
GitHub repository: Ktistos/queue-simulator-benchmark
branch: master
visibility: private unless explicitly changed later
```

Clone them side by side because some validation scripts receive the Kubesizer root as a path:

```bash
mkdir -p ~/work
cd ~/work
git clone --branch feature/statistical-simulation-workflow \
  https://github.com/Ktistos/kubesizer.git Kubesizer
git clone https://github.com/Ktistos/STELAR_LOAD_TESTS.git
git clone https://github.com/Ktistos/queue-simulator-benchmark.git
```

The raw experiment directory is large and is not suitable for normal source control. Source code, test plans, Kubernetes manifests, workload files, service-unit template, compact reports, and compact summaries are committed. Large request traces, Prometheus dumps, Locust CSV files, virtual environments, build output, credentials, and caches remain outside Git.

To continue the live experiment from another computer, copy the SSH private key securely through a password manager or another private channel. Do not commit it.

## 16. What can honestly be claimed today

Kubesizer now has the architecture needed to produce evidence-driven initial replica recommendations:

- fast native simulation;
- richer queue, concurrency, call, distribution, timeout, and network semantics;
- exact and scalable optimization;
- calibrated independent replications;
- confidence-aware feasibility;
- fresh model-level validation;
- real-measurement readiness checks;
- reproducible load-test experiments.

It has produced a useful recommendation in a controlled heavy-tail deployment and has correctly exposed important failures of the simpler analytical model.

It is not yet honest to claim that every recommendation will be correct for every Kubernetes deployment. The running deployment-family study is intended to widen that evidence. Even after it completes, the guarantee must remain conditional on representative measurements and on the model containing the important bottlenecks.

The safe current wording is:

> Kubesizer produces a defensible initial sizing recommendation for a measured workload when its evidence and predictive-validation gates pass. Otherwise it returns an explicit limitation or failure instead of an unsupported replica count.

## 17. Main remaining work

1. Let the 405-run deployment-family study finish.
2. Verify that every namespace was restored to its original replica count.
3. Run the study analysis and inspect family-by-family prediction error, feasibility decisions, confidence intervals, and censored requests.
4. Compare simulator recommendations with the measured smallest reliable configurations.
5. Tighten or reject model semantics for any family that fails.
6. Decide practical default safety margins from evidence, not from one topology.
7. Repeat on at least one real deployment with different dependencies and workload behavior.
8. Remove temporary experiment RBAC and credentials after the evidence is copied safely.

## 18. Session handoff note

This document preserves the useful context from the long Codex collaboration: the goal, rejected approaches, architecture choices, statistical rules, experiment design, infrastructure, paths, current status, tests, findings, and next actions.

It does not contain hidden model reasoning, access tokens, Kubernetes credentials, account passwords, or SSH private-key contents. Those would be unsafe and are not required to continue the engineering work. Git history preserves the code evolution; this report preserves why the important changes were made.

## 19. Detailed architecture map

This section explains where the main responsibilities live in the Kubesizer
source tree. It is intended for someone opening the repository for the first
time.

### 19.1 Workload and performance model

`performance_models/workload.py` parses and validates the workload description.
It handles request classes, concurrency weights, traffic distributions, service
demands, call semantics, latency components, deadlines, and SLOs. It also keeps
backward compatibility for older in-memory workload dictionaries where that can
be done without ambiguous meaning.

`performance_models/system.py` owns the service graph and the high-level
simulation interface. It calculates traffic, prepares the native payload,
invokes the required C++ executor, and converts the result into Python model
objects. It also exposes the analytical calculations used for stability and
fast evaluation.

The important design rule is that service identity and service order remain
stable. Replica vectors, state arrays, traffic matrices, and native-backend
payloads all depend on the same canonical service order.

### 19.2 Native simulator

`native_backend/backend.py` is the Python side of the native protocol. It finds
the required executable, checks the protocol, sends the input, handles process
errors, and parses the output.

`native_backend/cpp/src/main.cpp` contains the event kernel. It owns the future
event list, request lifecycle, service queues, replica dispatch, sampled
durations, call execution, deadlines, measurement tags, state integration, and
parallel replication worker pool.

`native_backend/README.md` documents the build and the public simulation
semantics. The normal build command is:

```bash
native_backend/cpp/build.sh
```

The C++ build needs a C++20 compiler, CMake, and Jansson. A sanitizer build is
available for memory and undefined-behavior checks.

### 19.3 Statistical calibration

The `calibration/` package contains pure statistical utilities and orchestration:

- profile options and immutable fingerprints;
- exact bucketization and ensemble reduction;
- warm-up cutoff rules;
- horizon and bucket-width sensitivity;
- measurement-duration studies;
- seed namespaces;
- calibration reports and diagnostics.

`statistical_workflow.py` presents the public workflow. It connects calibration,
configuration evaluation, exact or scalable search, and fresh final validation.

### 19.4 Characterization and evidence

The `characterization/` package converts portable DevOps measurements into model
inputs and judges whether that conversion is safe enough to use.

Important responsibilities include:

- parsing deployment, workload, and measurement evidence;
- choosing a defensible CPU-capacity basis;
- building empirical demand distributions;
- representing client, ingress, network, and residual latency separately;
- importing completed real-cluster experiments;
- keeping censored requests as lower bounds;
- evaluating measurement readiness;
- evaluating recommendation quality over independent observed runs;
- producing an initial sizing recommendation only when required evidence gates
  pass.

The newly added `experiment_io.py`, `latency.py`, `quality.py`, and
`readiness.py` make these responsibilities explicit rather than hiding them in
one large workflow.

### 19.5 Optimization

The `optimization/` package separates:

- evaluator feasibility status;
- exact search;
- scalable search;
- node placement;
- mode selection;
- result metadata;
- lower bounds and honest guarantee fields.

The old A* file remains only where needed for compatibility or history; it is
not the preferred replica-sizing path.

### 19.6 User interface and progress

`kubesizer_cli.py` exposes the single Kubesizer command. The user selects the
replica evaluator (`analytical` or `simulation`), replica search (`exact`,
`scalable`, or `auto`), and placement search (`exact`, `scalable`, or `auto`).
Node placement can be skipped when no resource file is provided or when the
skip flag is used.

The normal user output is deliberately small:

- current stage;
- progress;
- elapsed time and estimated remaining time;
- final replica recommendation;
- optional node placement;
- clear failure or uncertainty.

Debugging and scientific detail remain available as returned objects and
diagnostics rather than being printed mindlessly.

`runtime_estimation.py` estimates stage ceilings from workload size, replica
bounds, statistical settings, search mode, placement space, measured kernel
rates, and configured budgets. It provides an estimate, not a promise. Heavy
tails, long draining, failed warm-up convergence, and system load can make a run
take longer.

## 20. Detailed request lifecycle in the native simulator

The request lifecycle is important because a small mistake here changes queue
lengths, utilization, and end-to-end latency.

### 20.1 External arrival

Each request class has one or more external streams. An external arrival:

1. creates one end-to-end request;
2. records its original external arrival time;
3. increments the network-wide active-request count exactly once;
4. decides whether its arrival time belongs to the measurement cohort;
5. enters the class's configured entry service or call graph;
6. schedules the next external arrival from the same renewal stream.

Internal calls do not increment the end-to-end active count. A request may visit
ten services and still count as one active external request.

### 20.2 Service admission and processing

A service visit first reaches a selected replica or the shared service queue,
depending on dispatch policy.

With fixed slots:

- if an admission/concurrency slot and a processing slot are available, service
  starts immediately;
- otherwise the visit waits in the correct FIFO queue;
- service completion frees processing capacity;
- the next eligible waiting visit starts at the same simulation time;
- when synchronous downstream calls exist, the parent can release its CPU slot
  while still holding the configured request-concurrency slot.

With processor sharing, active work shares the configured processing capacity.
The event kernel updates the remaining work whenever the active set changes.

Independent replica queues are different from a pooled queue. If a request was
sent to a busy pod, a different idle pod cannot steal it unless the selected
model explicitly represents such a balancer. This can matter near capacity.

### 20.3 Downstream calls

After a service visit completes its local work, the request may:

- exit;
- move to one next service;
- revisit an earlier service;
- issue several sequential calls;
- issue several parallel calls.

Network transit is sampled on the call boundary. Before placement is known, the
workload's cross-node probability selects the same-node or cross-node empirical
distribution. After placement is attached, the actual source and destination
nodes select the directional distribution.

Parallel children join before the parent continues. Sequential children run in
declared order. These choices are never inferred from aggregate edge counts
because edge counts do not reveal timing.

### 20.4 Deadline and final exit

If a declared end-to-end deadline expires, the user-visible outcome is recorded
at exactly that deadline and marked as a failure. Server work may continue, which
is realistic for requests where client cancellation does not immediately stop
all downstream processing.

Without a deadline, an eligible request is followed until normal final exit.
Final exit:

1. records end-to-end latency from the original external arrival;
2. records request-class completion and failure state;
3. decrements the active-request count exactly once;
4. asserts that the count did not become negative.

### 20.5 Same-time events

Events have a deterministic ordering key:

```text
(event time, event priority, sequence number)
```

The sequence number preserves FIFO order when time and priority are equal. State
history changes at the same timestamp are coalesced so each history retains only
the final value at that timestamp. This avoids zero-duration intermediate states
changing time-weighted areas.

## 21. Detailed warm-up method

An empty simulation begins with no requests and no queues. A steady real service
normally does not. Warm-up removes the part of the run that is biased by this
artificial empty start.

### 21.1 What is monitored

For every replication, Kubesizer tracks:

- total active end-to-end requests;
- each service's waiting queue;
- each service's busy capacity;
- derived number in system;
- derived utilization.

Latency p95 is not the primary warm-up signal because a small time bucket may
contain too few requests for a stable p95.

### 21.2 Exact time buckets

The event history says that a state value begins at an event time and lasts until
the next event. To calculate a bucket average, the implementation multiplies
each value by the exact amount of time for which it was active inside that
bucket. Intervals crossing bucket boundaries are split. The last state is
flushed to the pilot horizon even if no event happens exactly there.

This is more accurate than looking only at the bucket boundary. A queue could
grow and disappear between two snapshots; boundary sampling would completely
miss it.

### 21.3 Ensemble and cutoff

Corresponding buckets are averaged across independent replications. For each
important metric:

1. the final 20% provides a provisional late reference;
2. non-overlapping analysis windows are calculated;
3. three consecutive windows must stay within a practical tolerance;
4. a fitted trend across those windows must also be small;
5. the earliest passing window becomes that metric's candidate cutoff.

The largest metric cutoff controls the global candidate. One safety window is
added. The result must still be far from the end of the pilot and leave enough
post-cutoff buckets.

The same long trajectory can be viewed at `H` and `2H`, and at compatible
bucket widths. This avoids rerunning a prefix that already exists. Calibration
is accepted only when horizon and bucket-width changes do not materially change
the selected `W`.

Representative larger configurations receive fresh-seed validation. Kubesizer
does not calibrate a different warm-up for every optimizer candidate.

### 21.4 Why calibration can fail

Failure is expected when:

- the mathematical network is unstable;
- utilization is so close to one that the transient is extremely long;
- the late reference is still moving;
- too few replications make the earliest-window detector unstable;
- traffic or service behavior changes over time;
- heavy tails or hidden modes are longer than the pilot;
- the model contains a routing loop that does not terminate safely.

Choosing an arbitrary warm-up is acceptable for an early exploratory prototype,
but it cannot support a strong claim that empty-start bias is controlled.

## 22. Detailed p95 and confidence method

Suppose there are 30 independent simulation replications. Each replication
contains many requests. Requests inside the same run share the same periods of
congestion, so treating every request as a fully independent statistical sample
would make the interval falsely narrow.

Kubesizer keeps the latency array from every replication separately. The point
p95 is computed from all eligible requests pooled together. For one bootstrap
resample, it selects 30 replication indices with replacement, includes each
selected replication's complete latency array, pools those selected arrays, and
computes p95. Repeating this produces a distribution of plausible p95 values.

This method has two useful properties:

- the point estimate is the requested steady-state request quantile, not the
  average of 30 unrelated per-run p95 values;
- resampling preserves within-run dependence at the replication-cluster level.

The bootstrap random seed belongs to a separate namespace. Changing bootstrap
resamples cannot change the simulated request paths.

Ordinary 95% intervals cannot be checked repeatedly at many optimizer steps and
still be called an overall 95% procedure. Kubesizer divides the allowed error
conservatively across:

- lower and upper tails;
- maximum sequential looks;
- required request classes.

This is Bonferroni control. It is conservative, so intervals may be wider, but
its meaning is simple and defensible.

Very small bootstrap counts cannot represent very small tail probabilities.
The implementation checks resolution and fails clearly instead of returning an
interval whose requested confidence level the resampling count cannot support.

## 23. Detailed replica optimization

### 23.1 Analytical lower bound

The traffic equations compute each service's effective arrival rate, including
internal visits. The strict minimum stable count is the smallest integer replica
count with total mean service capacity above that effective rate.

This vector is a lower bound. Kubesizer never removes replicas below it. It is
not automatically SLO-feasible.

### 23.2 Exact mode

Uniform-cost search starts from the stable vector. Each edge adds one replica
and incurs its configured cost. States are processed in increasing total cost.
The evaluator, not the analytical ranker, decides SLO feasibility.

An exact result is globally minimum-cost inside the bounded model only when all
cheaper states were resolved. If a cheaper simulation state remains
`INCONCLUSIVE`, a more expensive candidate may still be returned as feasible,
but minimality is explicitly not proven.

### 23.3 Scalable mode

The multi-start heuristic has four practical parts:

1. Start from the stable vector.
2. Add promising replicas using analytical violation reduction per cost, with a
   randomized restricted candidate list.
3. Once feasible, try every legal removal and retain only removals that remain
   certified feasible.
4. Perturb the result by adding one to three replicas in selected places, then
   prune again. This can escape a basin where a simple deletion cannot improve
   cost but an exchange can.

Several seeded restarts explore different choices. The best certified feasible
incumbent never becomes worse. Evaluation count, total evaluator work, wall
time, restarts, and no-improvement all have explicit limits. Evaluated immutable
configurations are cached in memory within the run so the same candidate is not
simulated again unless its statistical evaluator explicitly requests more
replications.

Deletion-local status is only certified after a complete pass proves that every
remaining single removal is infeasible. An unknown result or exhausted budget
prevents that label.

### 23.4 AUTO and practical scale

AUTO estimates the complete bounded configuration count. For `n` services with
the same `b` allowed counts, a rough size is `b^n`. This grows quickly:

```text
5 choices for 5 services  = 3,125 states
5 choices for 6 services  = 15,625 states
5 choices for 10 services = 9,765,625 states
```

The exact threshold is configurable and runtime-adaptive. Service count alone
is not enough because tight bounds can make a larger service graph easy, while
wide bounds can make a small graph expensive.

## 24. Detailed node-placement optimization

Replica sizing chooses how many replicas exist. Placement assigns those fixed
replicas to nodes while respecting CPU, memory, affinity, anti-affinity, and
spreading constraints.

Placement does not need latency simulation unless placement changes the
latency model. In the current enriched simulation, placement can change
directional network delay and node slowdown, so the combined plan must receive
fresh final latency validation when those effects are enabled.

Exact placement uses branch-and-bound. It enumerates assignments but prunes a
partial assignment when its current or lower-bound cost cannot beat the
incumbent.

Scalable placement works as follows:

1. difficult replicas are placed first;
2. each partial beam state is expanded to capacity-feasible nodes;
3. a bounded, diverse beam retains promising different resource shapes;
4. completed placements undergo single-replica relocation descent;
5. pairwise swaps can improve full nodes where relocation is impossible;
6. small groups of costly replicas can be removed and jointly repaired;
7. multiple seeded starts preserve the best feasible incumbent.

A failed beam means only “this heuristic start found no placement.” It is not a
proof that the placement problem is impossible. Local certification is reported
only after a complete final relocation and swap scan.

The final user output lists both pieces clearly:

```text
recommended replicas by service
recommended node for every replica, when placement is requested
```

## 25. Detailed measurement semantics

The measurement repository uses operational laws carefully because several
numbers look similar but mean different things.

### 25.1 External rate

`gamma` is traffic entering the application from outside. In the current STELAR
cluster, there is no reliable request-operation label in Istio metrics. The
collector therefore reconstructs aggregate entry traffic using destination-side
Istio counters, unknown source identity, and a known entry-workload set.

It does not infer that every unknown source is a user. Probes and non-mesh callers
may contaminate this number. The output records the attribution method and this
limitation.

### 25.2 Service rate and edge rate

`lambda_i` is the rate arriving at service `i`. `C_ij` is the observed call rate
from service `i` to service `j`.

For a true one-next-hop router, `C_ij / sum_j C_ij` is a routing probability.
For microservices with fan-out, the more useful quantity is:

```text
visits from i to j per request handled by i = C_ij / lambda_i
```

This value can exceed one and outgoing values do not have to sum to one.

### 25.3 CPU demand

Application-container CPU usage is measured in CPU-seconds per second, which is
approximately consumed cores. Dividing by matched per-pod throughput gives
CPU-seconds per request:

```text
D_cpu = application CPU cores / requests per second
```

The join uses namespace and destination pod. Sidecar CPU is excluded.

This is an effective CPU demand under the measured resource and workload. It is
not the wall-clock time spent inside one request and does not include waiting
without CPU use.

If a defensible CPU capacity `C` exists, `C / D_cpu` gives a CPU-based capacity
estimate. It does not prove saturation throughput when another resource is the
bottleneck. When the deployment has no CPU request or limit, the current STELAR
experiment uses an explicit one-core normalization convention and records that
as a limitation.

### 25.4 Latency layers

The system keeps these layers separate:

- client end-to-end time;
- ingress-controller request time;
- application-entry Istio time;
- per-service Istio response time;
- CPU-effective queue demand;
- same-node and cross-node transit;
- explicitly unmodeled residual.

Subtracting independently aggregated percentiles is invalid. Differences are
only derived from aligned mean counter windows or request-correlated evidence.
Ingress duration often already includes upstream time, so adding it as a new
service queue would double count the application.

## 26. Why fixed overhead was introduced and then weakened

At low load, the queue-network prediction accounted for only part of observed
client latency. A fixed residual was calculated as observed low-load latency
minus predicted low-load queue-network residence. It represented everything not
otherwise modeled at that boundary: client transit, ingress, serialization,
baseline non-HTTP work, and other roughly constant delay.

A fresh low-load validation run showed that this residual generalized reasonably
to another low-load run. That was useful evidence, but only for the tested
condition.

Near capacity, the residual failed because the missing delay was not fixed. A
thread pool, hidden downstream database, connection limit, or heavy service-time
tail creates load-dependent delay. No constant can reproduce that curve.

The current model therefore prefers measurable components and distributions:

- client-to-ingress samples;
- ingress-processing samples;
- same-node network samples;
- cross-node network samples;
- a remaining residual only for what cannot yet be identified.

The workload contains example CSV files for these empirical components. The
evidence gate still requires holdouts because measuring more components reduces
model error but does not prove that every component was found.

## 27. Experiment design in more detail

The current deployment-family study is not simply “run the recommendation once.”
It is designed to answer several questions:

1. Does the simulator preserve the correct feasibility decision at low, target,
   high, and bursty load?
2. Does the selected replica vector pass fresh observations?
3. Do cheaper neighbors fail for the reason the model predicts?
4. Does a safety vector behave better when the target is close to a boundary?
5. Does the result change across single, fan-out, repeated-call, and shared
   multi-class shapes?
6. Are errors caused by the service or by a saturated load generator?

Each run stores its seed, exact configuration, distribution, warm-up, measurement
window, drain time, deployment state, client traces, and telemetry. The plan
fingerprint prevents resuming with a different experiment definition. The
original replica counts are recorded before the first treatment and restoration
runs in a `finally` path.

The analysis must keep two types of uncertainty separate:

- replication uncertainty: how results change across independent runs of the
  same declared model or deployment;
- model error: how the simulator prediction differs from the real cluster.

A narrow simulator interval can coexist with a very wrong real prediction. The
STELAR near-capacity study demonstrated exactly that possibility.

## 28. Test inventory in more detail

### 28.1 Native semantics

Tests cover one and several replicas, immediate service, queue formation, a
waiting request taking a released slot, routing, final exit, loops, multiple
entry streams, same-time events, in-flight termination, state coalescing,
timeouts, and all supported distributions.

The separate benchmark contains deterministic differential traces across all
five language kernels. The C++ deterministic traces also passed AddressSanitizer
and UndefinedBehaviorSanitizer.

### 28.2 Statistics

Tests use hand-calculated state areas, intervals that cross several buckets,
final-state flushing, ensemble means, near-zero tolerance floors, rising-then-
flat cutoffs, permanently increasing failures, horizon sensitivity, bucket-width
sensitivity, cohort boundaries, slow completion after the window, pooled p95,
complete-cluster resampling, bootstrap seed reproducibility, insufficient
bootstrap resolution, sequential looks, multiplicity adjustment, and fresh
final seeds.

### 28.3 Optimization

Replica tests verify the stability floor, upper bounds, heterogeneous costs,
feasible-only acceptance, unknown evaluation handling, deletion-local
certification, add-and-reprune escape, multi-start reproducibility, cache reuse,
budget termination, exact-versus-scalable gaps on small fixtures, and the rule
that the incumbent never becomes worse.

Placement tests verify CPU and memory capacity, relocation, full-node swaps,
pairwise communication improvements, LNS repair, beam failure semantics,
reproducibility, budget behavior, local certificates, and exact comparison on
small fixtures.

### 28.4 Real evidence and load generation

Tests verify Prometheus label discovery, external-entry ambiguity, operation
labels, pod joins, sidecar exclusion, Kubernetes quantities, histogram
aggregation, zero throughput, CPU demand, capacity estimates, errors, supported
arrival distributions, fixed seeds, endpoint profiles, cluster formation,
request-cohort reconstruction, authentication classification, censoring,
scheduler saturation, interrupted studies, resume behavior, restoration, and
study analysis.

## 29. Known limitations, stated plainly

1. Aggregate telemetry cannot identify all request-level dependence.
2. Istio HTTP edges cannot see database, cache, disk, or other non-HTTP work.
3. CPU demand cannot reveal wall-clock waiting or concurrency limits by itself.
4. A low-load residual does not predict load-dependent hidden contention.
5. Request-class telemetry is unavailable without a bounded operation label or
   controlled isolated runs.
6. A fixed queue-dispatch policy must be supplied; aggregate metrics cannot
   reliably infer it.
7. Sequential versus parallel fan-out must be known or tested as sensitivity
   variants.
8. Heavy tails need enough independent replications and a long enough drain.
9. A stationary model is not valid for a workload whose rates or behavior drift
   during the experiment.
10. The stable replica vector is a mathematical lower bound, not a latency
    guarantee.
11. A simulation feasibility certificate applies to its model, not automatically
    to Kubernetes.
12. A real-cluster validation applies to its measured workload, resource limits,
    software version, topology, and load range—not to every future deployment.
13. The scalable optimizers have no general global-optimality or approximation-
    ratio guarantee.
14. Runtime estimates are planning ranges, not deadlines.
15. The running multi-family study is not complete at this handoff snapshot.

These are not reasons to abandon the tool. They define when its answer is useful
and when it must ask for stronger evidence.

## 30. Reproduction checklist for a new computer

1. Install Git, Python 3.11 or newer, a C++20 compiler, CMake, Jansson, and
   Kubernetes tools if cluster experiments will be run.
2. Clone the three repositories described in this report.
3. Create separate Python virtual environments for Kubesizer and the load-test
   repository.
4. Build the native C++ backend.
5. Run the complete unit suites before changing code.
6. Obtain the experiment VM SSH key through a secure private transfer.
7. Check the system service and manifest; do not start a second copy of the same
   study.
8. After completion, copy the full study directory or at least the manifest,
   analysis, summaries, and required raw cohorts to durable storage.
9. Verify cluster restoration before deleting the temporary experiment access.
10. Remove the temporary RBAC and token only after evidence is safely copied.

Suggested local verification commands:

```bash
cd ~/work/Kubesizer
python3 -m pytest -q
native_backend/cpp/build.sh

cd ~/work/STELAR_LOAD_TESTS
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[load]'
python -m pytest -q
```

The native build and package-install commands require normal development
dependencies but no application source modification.

## 31. Repository history in plain language

The main development sequence was:

1. Create the service graph and first analytical solver.
2. Add the first simulation and metrics objects.
3. Add A* replica search and node-placement models.
4. Add analytical stability and minimum stable replicas.
5. Add request classes and concurrent traffic weights.
6. Add event-driven state histories and warm-up tools.
7. Discover that robust calibration was too expensive in SimPy.
8. Build and validate custom Python, Java, Go, and C++ kernels.
9. Adopt the C++ kernel as the required executor.
10. Add arrival-cohort draining, independent replications, p95 cluster
    bootstrap, sequential classification, and fresh final validation.
11. Replace weak A* sizing with uniform-cost exact search and scalable VNS.
12. Replace exhaustive placement as the only option with branch-and-bound and
    scalable beam/VNS placement.
13. Add AUTO/hybrid selection, runtime estimates, and concise progress.
14. Generalize workload distributions, request-class weights, SLO types, and
    evaluation modes.
15. Build the Istio/Prometheus/cAdvisor measurement pipeline and Locust load
    generator.
16. Test the original analytical model against STELAR and reject it as the sole
    oracle near capacity.
17. Add operation-aware demand, call semantics, concurrency limits, network and
    ingress components, predictive evidence gates, and safe refusal.
18. Validate enhanced simulation on controlled heavy-tail sizing cases.
19. Start the larger independent deployment-family study now running on the VM.

This order explains why the repository contains both analytical and simulation
paths. The analytical code remains valuable even though it is not a universal
latency oracle, and the exact algorithms remain valuable for small bounded
instances even though heuristics are needed for larger ones.

## 32. Glossary

**Arrival rate:** requests entering a boundary per second.

**Service demand:** work one request requires from a service. CPU demand is only
the CPU part of that work.

**Replica:** one deployable service copy or pod represented by the capacity
model.

**Stability:** the ideal queue has more long-run service capacity than arriving
work.

**SLO:** a declared latency target, such as mean below 150 ms or p95 below
500 ms.

**Warm-up:** early empty-start simulation time excluded from latency evidence.

**Measurement arrival window:** the interval during which newly arriving
requests become eligible for the result.

**Drain:** continuing the simulation or load test until eligible slow requests
finish.

**Replication:** one independent run with its own simulation or load seed.

**Confidence interval:** a range reflecting uncertainty from finite independent
replications under the chosen statistical method.

**Cluster bootstrap:** resampling whole independent runs rather than pretending
every request in one run is independent.

**Analytical precheck:** a fast equation-based stability or feasibility screen.

**Exact search:** a search that can prove the best bounded-model result when it
finishes and every required evaluation is resolved.

**Scalable search:** a budgeted heuristic intended to find a good certified
feasible result without visiting the full configuration space.

**Predictive holdout:** fresh real-cluster evidence that was not used to fit the
model.

**Censoring:** a measured request did not finish before observation stopped, so
only a latency lower bound is known.

**Residual:** measured latency not explained by the explicitly modeled
components. It is evidence of missing structure, not automatically a constant
law of the deployment.

## 33. Final handoff conclusion

The project has moved from a small M/M/c and A* prototype to an evidence-driven
capacity-planning system with a fast native kernel, richer workload semantics,
statistically controlled evaluation, scalable search, portable telemetry
measurement, reproducible load generation, and explicit refusal when evidence
is weak.

The most important lesson is not that C++ solved prediction. C++ solved the cost
of running many detailed simulations. Prediction improved because the workload
model, request lifecycle, concurrency semantics, latency boundaries, statistical
design, and real-cluster validation were also strengthened.

The current system is suitable for continued controlled experiments and for
conditional initial recommendations when its gates pass. The ongoing 405-run
study is the next major evidence point. Its outcome should decide which defaults
are defensible across the tested deployment families and which model assumptions
still need to be changed.
