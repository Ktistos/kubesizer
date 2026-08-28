# Statistically controlled simulation workflow

The production experiment has three deliberately separate statistical stages
and a prior real-cluster model-validation gate. The
`kubesizer` command uses the public functions in `statistical_workflow.py`; it
does not load calibration profiles and has no persistent evaluation cache.

## Public interface

```python
from statistical_workflow import (
    calibrate,
    evaluate_configuration,
    recommend_replicas_with_validated_simulation,
)
from predictive_validation import validate_simulator_predictions

# `holdout_cases` contain fresh observed means or p95s and observed error rates.
predictive_certificate = validate_simulator_predictions(
    system,
    holdout_cases,
    options=predictive_options,
)
predictive_certificate.assert_valid_for(system)

profile = calibrate(
    system,
    request_class_slos={"checkout": 1.0},
    maximum_replicas=10,
    report_directory="simulation_calibration_output",
)

evaluation = evaluate_configuration(system, configuration, profile)
recommendation = recommend_replicas_with_validated_simulation(
    system,
    profile,
    predictive_certificate,
)
if not recommendation.recommendable:
    raise RuntimeError(recommendation.guarantee)
```

Portable DevOps measurements use a separate side-effect-free preparation and
evidence-gating interface:

```python
from characterization import (
    evaluate_closed_loop_recommendation,
    load_and_prepare_initial_sizing,
)

prepared = load_and_prepare_initial_sizing(
    "deployment.yaml",
    "workload.yaml",
    "measurements.yaml",
)
prepared.risk_assessment.assert_recommendable()

# Each item is one independent post-warm-up real-cluster load-test run.
quality = evaluate_closed_loop_recommendation(
    prepared.recommendation.configuration(service_order),
    observed_runs,
    {"browse": 0.25},
    latency_statistic="mean",
    declared_configuration_space=tested_configuration_space,
)
```

The preparation result includes per-service measured CPU-demand and load
ranges, an estimated CPU-utilization range, load/demand correlation,
capacity-run coverage, and findings classified as conditional or blocking. A
weak result remains inspectable through lower-level functions, but the public
preparation workflow raises instead of presenting insufficient evidence as a
defensible recommendation.

The closed-loop runner receives deployment and load-generation callbacks;
Kubesizer does not assume Helm, Argo, a particular Kubernetes API client, or
one load generator. It applies configurations sequentially and restores the
original deployment in a `finally` block. The analysis resamples whole load-
test runs and adjusts its tail probability across all tested configurations
and constraints. It reports the smallest observed passing configuration. It
calls that minimum certified only if the caller declares the complete finite
comparison space and every cheaper configuration is measured and conclusively
infeasible.

Existing load-test directories can be imported with
`load_observed_experiment_replications`. The importer uses actual client-start
time to remove warm-up and post-window trace rows, retains eligible requests
that completed after the arrival window, verifies the declared optimizer
service order against Kubernetes replica counts, and rejects saturated load
generators. An incomplete request is represented by its latency lower bound at
the recorded experiment stop and an explicit censor marker. Such lower bounds
may establish infeasibility when even their confidence lower bound exceeds the
SLO; a cohort containing any censoring can never establish feasibility. A
censored request is not silently relabelled as an HTTP failure.

`CalibrationOptions` controls pilot replications, tolerances, sample minima,
`R_min`, `R_max`, batch size, alpha, bootstrap resamples, drain protection and
all seed namespaces. Defaults are scientific starting points, not universal
constants.

## Prior gate: does the simulator represent the cluster?

Monte Carlo confidence intervals show uncertainty inside the simulator; they
cannot show that the simulator represents Kubernetes. Before a plan is called
a recommendation, `validate_simulator_predictions` runs the frozen model on at
least two fresh real-cluster holdouts, including the exact target workload. It
checks every request class against directional practical-error tolerances,
requires a sufficiently precise simulation interval, and rejects observations
whose failures are not represented by an explicit request deadline. When a
deadline is declared, timeout outcomes remain in the latency sample and their
replication-cluster-bootstrap failure rate must also match the holdout. The
immutable result is fingerprinted to the workload, topology,
service demands, call semantics, capacity semantics, and simulator version.

`recommend_replicas_with_validated_simulation` requires an accepted
certificate, runs calibrated optimization, and then performs the separate
fresh-seed Stage C validation. This yields either a validated bounded-model
optimum, a validated feasible heuristic, or no recommendation. “Bounded-model
optimum” never means globally optimal for unmeasured live-cluster behavior.

The simulation-ready workload can represent an ordered workflow or a
synchronous call graph. A call graph must explicitly choose `parallel` or
`sequential` sibling execution. Each replica contributes separately configured
processing slots and admitted-request concurrency. A parent releases its
processing slot but retains its concurrency slot while waiting for downstream
calls. This captures CPU contention and worker/thread exhaustion as different
mechanisms. Mean visit ratios remain analytical-only because edge telemetry
does not reveal call order or synchronization.

## Stage A: calibration

The analytical open-network traffic equations establish the strict minimum
stable configuration. Warm-up calibration runs empty-start native replications
there, computes exact time-weighted buckets from event-driven active, queue and
busy state, and analyzes active requests, per-service queue length,
number-in-system and utilization. It uses late references, practical tolerance
floors, three stable windows, a trend check, a safety window, horizon doubling,
bucket-width sensitivity and fresh validation across representative bounded
configurations. A failed diagnostic produces no numerical `W`.

Measurement calibration is a separate arrival-cohort experiment. It estimates
`M0` from the slowest required external request-class rate and studies
`M0, 2*M0, ...`. One longest run per configuration is reused for compatible
shorter arrival cohorts. Each candidate must meet total/nonempty sample minima,
a p95 CI precision target, p95 stability under doubling and a practical
CI-half-width stability condition. Every eligible request arriving in
`[W, W + M)` is drained to completion; pre-warm-up and post-window arrivals are
excluded from latency observations but continue to affect the network.

The immutable YAML profile fingerprints service identities, routing,
request-class definitions, arrival and service rates, SLOs, simulator semantics
and replica bounds. Current replica counts are deliberately absent. A changed
fingerprinted input invalidates the profile; the public API rejects an
incompatible profile rather than silently using it.

## Stage B: optimization evaluation

Each analytically stable candidate starts independent empty-state replications,
runs continuously through frozen `W`, measures the frozen `M` cohort and drains
it. Configurations use common random numbers: replication index `r` uses the
same simulation seed across configurations while replications within one
configuration remain independent.

The p95 point estimate is the pooled eligible-request p95, not the mean of
per-replication p95 values. Confidence bounds use a percentile bootstrap that
resamples complete replication clusters, preserving within-run dependence.
Simulation seeds and deterministic bootstrap seeds occupy disjoint namespaces.

At most `K` looks are permitted. A fixed two-sided Bonferroni tail allocation

```text
alpha_tail = overall_alpha / (2 * K * request_class_count)
```

controls repeated looks and multiple request classes conservatively. A
configuration is statistically feasible only when every upper bound is at or
below its SLO. One lower bound above its SLO is statistically infeasible.
Overlapping intervals cause another fixed batch until `R_max`, then remain
`INCONCLUSIVE`. Analytical instability, simulation failure and calibration
failure are separate classifications. The replica search accepts only
`STATISTICALLY_FEASIBLE` as a goal.

Replica sizing uses uniform-cost search from the minimum analytically stable
configuration. Since every transition adds exactly one replica, states are
examined by the number of replicas added; heterogeneous replica costs can use
the same algorithm with their actual non-negative step costs. Analytical
bottleneck utilization orders candidates only within one cost level. After
finding a goal, the search finishes that cost level to select the feasible
configuration with the best confidence-bound/SLO margin. It never expands a
feasible state.

A feasible candidate is a proven minimum-cost result only when every cheaper
state was definitively classified. If a cheaper state reached `R_max` as
`INCONCLUSIVE`, optimization may still return a larger validated candidate, but
`minimality_proven` is false and `unresolved_lower_cost_states` records the
barrier. Final fresh-seed validation proves feasibility of the selected
candidate; it does not resolve those cheaper configurations.

## Stage C: final validation

The selected configuration is locked and rerun once at a fixed replication
count using only the final-validation seed namespace. No optimization samples
are reused and the holdout does not stop early. Its familywise interval uses
`overall_alpha / (2 * request_class_count)` per tail. The final claim passes
only when every fresh upper bound meets its SLO; placement is blocked otherwise.

## Evidence and limitations

Calibration output includes the immutable profile, JSON and Markdown reports,
measurement CSV, complete warm-up evidence, Welch diagnostic plots, p95 versus
`M`, CI width versus `M`, and optional sequential CI plots. Plotting and file
serialization are outside the timed native simulation kernel.

The stationary method must fail or be recalibrated for time-varying rates,
routing or service demands; unstable/open-network violations; nonterminating
routing; undrainable cohorts; unresolved warm-up transients; inadequate rare
classes; or a changed simulator-semantics version. Cluster percentile bootstrap
coverage is conservative through Bonferroni error allocation but remains a
finite-replication approximation, especially for very rare or heavy-tailed
classes. The kernel can represent a measured terminal deadline, but it cannot
infer the capacity that caused a backlog or explain retries, hidden databases,
or connection pools unless those resources and policies are explicitly measured
and modeled; an affected holdout rejects the predictive certificate.

Raw class-attributed arrival timestamps enable a moving trace-block bootstrap,
which preserves local burst ordering. Aligned arrival-rate and CPU-demand
windows are sampled as joint blocks, so a high-load/high-demand window is not
recombined with an unrelated low-load window. If CPU demand changes materially
with load and the target lies outside the measured load range, the evidence
gate rejects the recommendation instead of extrapolating a response curve.
Aggregate edge counters can identify repeated calls but cannot reveal sibling
ordering; the simulator evaluates explicit sequential and parallel sensitivity
variants.

No output field is a posterior probability that an SLO will pass. Simulation
confidence bounds quantify Monte Carlo and input-sampling uncertainty inside
the declared model. Predictive holdouts and closed-loop deployment experiments
provide the separate evidence that the model represents the real deployment.
