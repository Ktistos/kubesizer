# Retained-Data Validation of Richer Simulation Semantics

Historical selected-subset report. The [7 September corrected-code replay](corrected-replay-20260907.md)
reproduces its classifications but cannot recover excluded experiments. These
results do not establish unbiased safety or validate the corrected campaign.

## Conclusion

The new non-CPU blocking mechanism works, but **a blocking mean inferred from
CPU and Istio counters is not sufficient by itself**. On the accepted retained
deployment-family evidence, the basic telemetry-only model was less accurate
and produced more unsafe feasibility decisions than the existing baseline.

Adding two pieces of information changed the result materially:

1. the narrow resource/concurrency pool that blocking work occupies; and
2. an approximate blocking-time variability fitted from low-load Istio
   histogram mean and median.

That enriched sensitivity model made 17 of 18 configuration feasibility
decisions agree with the retained deployment results and produced no unsafe
false-feasible decision in this dataset. It was nevertheless conservative and
its numerical mean-latency error was worse. This is useful evidence, but it is
not sufficient to claim general predictive reliability.

## Experimental design

- No load experiment was rerun.
- Each model was characterized only from the retained, separate low-load run
  for its deployment family.
- Only retained study points that passed the original evidence gate were used.
- Eight load points, 18 configurations, and 22 request-class/configuration
  latency comparisons were included.
- Each prediction used 30 independent native C++ replications.
- Every replication used a 60-second warm-up and 120-second arrival-cohort
  measurement window with complete cohort draining.
- Models used common simulation seeds for each configuration.
- Mean-latency intervals used 20,000 replication-cluster bootstrap draws.
- Four target-validation points had 30 real deployment replications. Other
  accepted points normally had five.

The exact machine-readable inputs, fingerprints, simulation seeds, fitted
parameters, predictions, and limitations are retained in
`results/richer-simulation-validation.yaml`.

## Models compared

| Model | Inputs and semantics |
|---|---|
| Existing baseline | Synthetic workload's declared resource-holding service distribution plus a low-load fitted fixed request overhead |
| Telemetry CPU + blocking | Measured application CPU-seconds/request as processor-sharing work; remaining low-load Istio response as deterministic non-CPU blocking; declared admission concurrency |
| Telemetry + declared bottleneck | Same telemetry model, but blocking occupies the synthetic application's narrower configured processing pool |
| Telemetry + fitted variability | Declared-bottleneck model with a lognormal leaf blocking SCV fitted from low-load Istio mean and p50 |
| Oracle occupancy + blocking | Retains the synthetic deployment's privileged true resource-holding distribution and adds only the remaining low-load response residual as blocking |

The baseline and oracle models use synthetic ground truth that is not generally
available from cluster counters. The declared-bottleneck variants require a
DevOps-supplied worker/thread/connection/processing-pool limit. The fitted
variability model uses extractable Istio histogram values, but its lognormal
interpretation is an approximation rather than a direct observation of
per-request blocking.

## Results across all accepted points

| Model | Mean absolute error | MAPE | Feasibility agreement | Unsafe false feasible | Conservative false infeasible |
|---|---:|---:|---:|---:|---:|
| Existing baseline | 29.32 ms | 18.50% | 16/18 (88.9%) | 2 | 0 |
| Telemetry CPU + blocking | 77.39 ms | 36.83% | 10/18 (55.6%) | 8 | 0 |
| Telemetry + declared bottleneck | 31.24 ms | 23.89% | 15/18 (83.3%) | 3 | 0 |
| Telemetry + fitted variability | 68.39 ms | 43.39% | 17/18 (94.4%) | 0 | 1 |
| Oracle occupancy + blocking | 30.54 ms | 18.32% | 16/18 (88.9%) | 1 | 0 |

The fitted-variability model improved classification safety by moving uncertain
or overloaded cases away from false feasibility. It did not improve point
prediction: it often predicted much more queueing than was observed.

## Stronger 30-run target-validation subset

| Model | Mean absolute error | MAPE | Feasibility agreement | Unsafe | Conservative |
|---|---:|---:|---:|---:|---:|
| Existing baseline | 24.50 ms | 20.78% | 2/4 | 2 | 0 |
| Telemetry CPU + blocking | 29.83 ms | 23.41% | 2/4 | 2 | 0 |
| Telemetry + declared bottleneck | 24.16 ms | 19.79% | 2/4 | 2 | 0 |
| Telemetry + fitted variability | 32.20 ms | 28.07% | 3/4 | 0 | 1 |
| Oracle occupancy + blocking | 22.80 ms | 19.77% | 2/4 | 1 | 0 |

For the fitted-variability model, the remaining mistake was conservative: the
shared-service configuration was observed feasible but predicted infeasible.
The single-service and fan-out configurations that the old baseline incorrectly
accepted were correctly rejected.

## Replica-sizing comparison

Only four accepted load points established a minimum within their explicitly
tested configuration set. This is not a global minimum outside those sets.

| Model | Exact tested-minimum matches | Underprovisioned | Conservative |
|---|---:|---:|---:|
| Existing baseline | 4/4 | 0 | 0 |
| Telemetry CPU + blocking | 0/4 | 4 | 0 |
| Telemetry + declared bottleneck | 3/4 | 1 | 0 |
| Telemetry + fitted variability | 4/4 | 0 | 0 |
| Oracle occupancy + blocking | 4/4 | 0 | 0 |

The fitted-variability model matched the same four tested minima as the existing
baseline. Therefore these data do **not** establish an improvement in replica
recommendation quality. They do establish that CPU demand plus a mean residual,
without the occupied bottleneck resource and its variability, is unsafe.

## Correctness issue found during validation

The former stability precheck considered processing work only. A workload could
therefore be CPU-stable while a finite blocking/concurrency pool was overloaded.
The implementation now includes the necessary lower bound

```text
sum_k(lambda_ki * (mean_processing_ki + mean_blocking_ki))
---------------------------------------------------------------- < 1
             replicas_i * concurrency_limit_i
```

for every finite-concurrency service. Synchronous downstream waiting can retain
the parent concurrency slot even longer, so the calculated value is explicitly
a lower bound: a value at or above one proves instability; a value below one
does not prove stability.

## What the evidence supports

- Explicit non-CPU blocking is a meaningful simulator mechanism.
- CPU-seconds/request alone does not reveal a non-CPU serial bottleneck.
- A configured concurrency/resource-pool limit materially improves decisions.
- Low-load histogram variability can make sizing safer, but the current
  lognormal fit is too conservative for some families.
- The current richer model is not yet proven generally more accurate than the
  baseline.

## Next methodological step

The workload contract needs an explicit, DevOps-supplied resource-holding model:

- CPU demand and CPU capacity;
- blocking duration;
- which bounded pool the block retains (worker, connection, lock, or other
  concurrency resource);
- that pool's per-replica capacity;
- a measured or sensitivity-bounded variability model.

When only aggregate counters exist, Kubesizer should evaluate a bounded set of
plausible blocking variability/concurrency scenarios and accept a replica
configuration only if all supported scenarios pass. It should not treat one
mean residual as an identified service-time distribution.
