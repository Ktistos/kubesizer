# Portable fixed-configuration replay

Use `replay_portable_predictions.py` for offline diagnostics. Use the normal
`kubesizer` CLI for calibrated sizing and fresh validation. A diagnostic result
is not an SLO certificate or a replica recommendation.

The runner calls `load_portable_sizing_inputs`, `characterize_deployment` and
`build_characterized_system_variants` directly. It does not construct a frozen
model itself, substitute parallel child means, inject a residual, disable input
uncertainty or override the production bootstrap-block rule.

```bash
python3 scripts/replay_portable_predictions.py \
  --manifest replay.json --out results/my-new-replay
```

`--out` must be a new directory. Paths in the manifest are relative to that
manifest, unless absolute. Build the native backend before running; an isolated
executable can be selected with `KUBESIZER_CPP_BACKEND`.

Minimal manifest:

```json
{
  "seeds": [101, 102, 103, 104, 105],
  "warmup_seconds": 120,
  "measurement_seconds": 600,
  "maximum_drain_seconds": 30,
  "cases": [{
    "id": "api-two-replicas",
    "workload": "workload.yaml",
    "measurements": "measurements.yaml",
    "configuration": {"api": 2},
    "arrival_process": "stationary"
  }]
}
```

The configuration must name every service exactly once, using positive integer
replica counts. The workload and measurements are ordinary portable inputs,
not frozen simulator YAML. Forecast load belongs in the portable workload.

By default, arrivals come from the measurement package. To use one independent
load-test repetition for arrival shape, add this to a case:

```json
"actual_arrivals": {
  "traces": ["input-run/request_trace.browse.csv"],
  "start": 1000.0,
  "end": 1600.0,
  "expected_started_count": 12000
}
```

Copy the boundaries and count from that run's recorded cohort, and retain its
hash/provenance. Trace columns must include `request_sequence`, `request_class`
and `started_epoch`. The half-open window uses **actual starts**, including
requests that later fail. Scheduled timestamps are never a fallback. A class
with multiple entry services additionally needs attributable `entry_service`
values. Do not mix independent runs into one continuous arrival trace.

Never supply holdout latency observations to the model. Reusing an arrival
trace from a held-out outcome run is not an independent prediction experiment.

## Outputs and limits

- `predictions.json`: per-case and per-variant results, production-built models,
  assumptions, finite-window means and a latency-drift diagnostic.
- `bindings.json`, `protocol.json`, `integrity.json`: input/source/binary hashes,
  replay settings, and unchanged-source verification.
- `abstained`: missing/incompatible characterization, unsupported arrival
  semantics, failed capacity/drain checks, or growing/inconclusive cohort
  latency. Reasons are preserved; cases are not silently dropped.
- `finite_window_complete`: all variants completed and the diagnostic did not
  detect material early/late latency drift. This **does not prove stationarity**
  or certify the SLO. The runner reports `production_certified: false` and does
  not perform optimizer search, warm-up calibration or fresh final validation.

Declare `arrival_process: stationary` only when supported by the experiment
design. Chronological rate-step histories require a different, explicitly
time-varying replay contract; random-phase bootstrap is not that contract.

Historical adapters under `results/` remain immutable for reproducibility.
Their outputs must not be relabeled as current production-workflow results.

The regression run and its limitations are documented in
[the 28 October report](../results/production-aligned-replay-20261028/REPORT.md).
