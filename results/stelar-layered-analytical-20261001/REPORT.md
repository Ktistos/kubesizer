# STELAR analytical semantics replay

## Outcome

The enriched approximation is not an accuracy improvement and is not promoted
to master. The analytical-only baseline remains master commit `39309bd`.
The frozen experimental implementation is `2e6adf9`; the ISO-timestamp loader
correction and final pre-scoring revision is `67302af`. No formulas, pool
limits, residuals or workload parameters were tuned after scoring.

| Method | Mean MAPE, 4 healthy runs | Diagnostic false-feasible classifications, all 8 runs |
|---|---:|---:|
| Baseline relaxed diagnostic | 21.97% | 2 |
| Experimental layered, declared-pool sensitivity | 260.74% | 0 |
| Layered, unknown/unbounded-pool sensitivity | 31.97% | Not a pool-capacity safety classification |

The baseline's strict production workflow rejects these blocking/parent-holding
models. Its two false-feasible classifications above belong to the relaxed
diagnostic, not to emitted production recommendations. Likewise, enriched
classifications are conditional approximate model decisions, not certificates.

## Every target run

Latency is the client mean, in milliseconds. Unstable means no finite
stationary layered prediction. The source rate-56 points are retained in the
safety comparison but excluded from healthy-latency MAPE because of failures.

| Operation | Nominal rate | Repeat | Observed | Baseline | Layered | Failures |
|---|---:|---:|---:|---:|---:|---:|
| dataset_search | 32 | 1 | 146.07 | 162.72 | 1233.47 | 0% |
| dataset_search | 32 | 2 | 124.84 | 161.78 | 377.66 | 0% |
| dataset_search | 56 | 1 | 2653.10 | 1181.88 | Unstable | 3.61% |
| dataset_search | 56 | 2 | 2665.36 | Unstable | Unstable | 2.69% |
| organizations | 32 | 1 | 105.14 | 130.94 | 156.29 | 0% |
| organizations | 32 | 2 | 107.21 | 131.15 | 157.97 | 0% |
| organizations | 56 | 1 | 4760.03 | 155.00 | Unstable | 27.51% |
| organizations | 56 | 2 | 5083.44 | 171.13 | Unstable | 27.62% |

Predictions use actual scheduled offered rates, not nominal rates or reduced
completed throughput. Exact rates and replica vectors are in replay.json.
The original mean SLO is 500 ms. The layered method falsely rejects one of the
four healthy cases, while marking all four failed high-load cases unstable.

## Why the enriched result is poor

For dataset_search at approximately 32.47 offered RPS, the approximation puts
the parent pool at 98.4% holding utilization. Its waiting-time term then grows
sharply, producing 1233 ms where the observed mean is 146 ms. The other repeat
places that pool at 92.2% and predicts 378 ms. This is a sensitive, inaccurate
decomposition in these data, not evidence that the actual pool was saturated.

The result cannot identify whether the problem is the historical worker-count
assumption, the one-core CPU-capacity assumption, CPU/admission coupling,
non-independent downstream durations, or load-dependent service demand.
Removing the pool assumption produces 31.97% MAPE, still worse than baseline.
Fewer unsafe classifications achieved by overprediction do not establish a
better latency predictor or a cheaper safe recommendation.

## Input and evaluation discipline

Only two rate-4 characterization runs per operation supply application CPU work,
destination response means, child-call counts, arrival SCV and boundary residual.
CPU aggregation is total matched CPU / total requests across common windows and
replicas. Window variation is not falsely labelled per-request CPU variance.

Both methods receive identical deterministic local blocking estimates and
client-to-entry residuals. Frozen residual means are 75.995 ms for dataset_search
and 63.127 ms for organizations. They are not re-fit on target latencies.
Negative/unseparable leaf residuals are omitted and recorded in replay.json.
The old multi-load CPU regression is deliberately not used.

Capacity caveats are important: the raw collector used one-core normalization
and did not record CPU requests. Historical ckan=2 and stelarapi=4 worker counts
are declared sensitivities, not verified application-pool telemetry. Therefore
this is not an unconditional portable capacity validation.

The comparison is retrospective, with only four healthy mean-latency cases.
Historical outcomes were previously inspected. It is not a blinded holdout,
does not establish statistical significance, and does not validate real
fork-join semantics: both STELAR operations have sequential call assumptions.

## Reproduce

```sh
python scripts/replay_stelar_analytical.py \
  --data-root /home/ktistos/work/STELAR_LOAD_TESTS/experiments/operation-characterization/2026-08-22-admin-valid \
  --out results/stelar-layered-analytical-20261001/replay.json
```

All source hashes, frozen models, assumptions, per-run predictions, failures,
capacity diagnostics and strict baseline incompatibilities are in
[replay.json](replay.json). No live cluster workloads or source data were changed.
The [protocol](../../docs/STELAR_REPLAY_PROTOCOL.md) and
[semantics documentation](../../docs/EXPERIMENTAL_ANALYTICAL_SEMANTICS.md)
record the limitations independently of the outcome.
