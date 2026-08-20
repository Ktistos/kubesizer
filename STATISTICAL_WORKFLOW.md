# Statistically controlled simulation workflow

The production experiment has three deliberately separate stages. The
`kubesizer` command uses the public functions in `statistical_workflow.py`; it
does not load calibration profiles and has no persistent evaluation cache.

## Public interface

```python
from statistical_workflow import (
    calibrate,
    evaluate_configuration,
    optimize_with_calibrated_simulation,
    validate_final_configuration,
)

profile = calibrate(
    system,
    request_class_slos={"checkout": 1.0},
    maximum_replicas=10,
    report_directory="simulation_calibration_output",
)

evaluation = evaluate_configuration(system, configuration, profile)
optimization = optimize_with_calibrated_simulation(system, profile)
final = validate_final_configuration(
    system,
    optimization.configuration,
    profile,
)
if not final.passed:
    raise RuntimeError(final.failure_reason)
```

`CalibrationOptions` controls pilot replications, tolerances, sample minima,
`R_min`, `R_max`, batch size, alpha, bootstrap resamples, drain protection and
all seed namespaces. Defaults are scientific starting points, not universal
constants.

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
classes.
