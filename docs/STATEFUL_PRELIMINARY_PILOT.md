# Stateful mechanism pilot (sfp1)

Prepared 2026-11-16 on `feature/stateful-performance-models`. Collection is
authorized on Okeanos; this document defines the pilot, not its results.

The pilot tests a controlled key-value application deployed as three separate
one-member StatefulSets, each with its own persistent SQLite database. It
uses application-level HTTP propagation. It does **not** implement PostgreSQL
streaming replication, MinIO erasure coding, elections, recovery, consistency
guarantees or a general database model.

The primary and two followers use different Kubernetes worker nodes. Reads
select exactly one follower (probabilities 0.75 and 0.25). Every write runs at
the primary and propagates to both followers. The primary acknowledges writes
after local completion (`async`), after one follower (`quorum1`), or after
both (`all`). Unacknowledged branches keep consuming capacity. Persistent
membership stays fixed; this pilot does not establish scaling to new layouts.

## Measurements and frozen comparisons

The external mix is 75% reads and 25% writes, with Poisson arrivals. Two
low-load characterization runs supply class-specific samples of local worker
occupancy, RPC transport time and the client-to-entry boundary residual.
Local samples exclude worker queue waiting and downstream waiting, and include
the CPU/SQLite/storage work during which the one FIFO worker is occupied.
This explicit phase instrumentation is additional evidence; generic Istio and
CPU counters alone do not identify it.

Models use those empirical samples directly. They do not assume that a
configured CPU request equals measured capacity. SQLite I/O within a worker
is lumped into occupied worker time; shared-storage contention between
members is not independently modeled. That remains a possible model error.

The two simulation variants use identical measured demands, transport,
residuals, topology, resource counts and external rates:

- The legacy call-rules ablation has independent fractional read calls and
  waits for every write branch. It retains role-specific resources; it is
  **not** a comparison against every feature of the historical release.
- The enriched model uses exclusive read choice and the actual per-source
  acknowledgement rule.

Predictions for every target cell are frozen before collecting any target
outcomes. Each prediction uses 20 fresh simulation replications. This is a
fixed-configuration prediction comparison, not the full recommendation CLI
or a final feasibility certificate. The standard recommendation workflow
remains unchanged. Analytical evaluation is attempted with the strict
compatibility checks; finite synchronous parent holding causes abstention.
No relaxed analytical point prediction is silently substituted.

## Target matrix and time budget

| Factor | Values |
|---|---|
| Acknowledgement | Local/background; one follower; both followers |
| Offered mean worker-load fraction | 0.25, 0.60, 0.90 |
| Physical repetitions | 2 per cell; 18 target runs |
| Warmup | 60 seconds per target |
| Measurement | 240 seconds per target |
| Maximum cohort drain | 90 seconds; other traffic continues until the cohort finishes |
| Cooldown | 15 seconds after work has drained |
| Mean SLOs | Read: 0.25 seconds; write: 0.20 seconds |

The reference rate is the inverse of the largest offered local-worker demand
coefficient, calculated using characterization alone. These fractions are
design labels for **worker occupancy**, not observed CPU utilization or a
proven empirical saturation point. Actual worker occupancy, CPU work,
pending propagation and queue trajectories are retained.

The target windows alone require 90 minutes. Characterization, queue drains,
cooldowns, prediction freezing, provisioning and waiting for a comparison
checkpoint add time. The initial end-to-end estimate is **3–5 hours**.
Two physical repetitions are exploratory; they cannot establish reliable
tail behavior or a low unsafe-decision probability.

## Isolation, lifecycle and evidence

The namespace is `kubesizer-stateful-pilot-sfp1`. The VM collector receives a
Role in this namespace; no administrator credential is copied to the VM.
The controller waits for a completed comparison treatment with its evidence
receipt, then stops the comparison service. It verifies that comparison
deployments are scaled to zero before provisioning the pilot. A following
treatment may have begun setup during this transition; any partial attempt
is retained by the existing runner and rerun when resuming.

Completed comparison artifact bindings are checked across the pause. Frozen
comparison implementations, plans, input data and model receipts are not
edited. The pilot has its own code snapshot, deployment manifest, namespace,
protocol, predictions, random seeds and output directory.

At completion or failure, cleanup scales only this pilot's members to zero,
waits until its pods disappear, and restarts the original comparison service.
A systemd `ExecStopPost` repeats this cleanup after an unexpected process
exit. If pilot pods remain, the controller refuses overlapping experiments.
The service has a runtime limit; it does not wait on this conversation.

Files retained on the VM:

- `status.json`: current phase, active target and completed count.
- `protocol.json`, `source-receipt.json`, `deployment-context.json`.
- `characterization.json`, `models/`, `predictions-frozen.json`,
  `prediction-receipt.json`, `target-order-frozen.json`.
- `runs/*/requests.jsonl.gz`, `events.json.gz`, `occupancy.json`,
  `summary.json` and content-hash receipts.
- `target-results.json`, `scores.json`, `REPORT.md`.
- `comparison-pause.json`: preserved checkpoint and resume status.

Reports retain failures, delayed or rejected client requests, missing evidence
and analytical abstentions. Latency error eligibility requires complete runs
without request/propagation failures and without material client scheduling
delay. Failed outcomes remain visible outside the latency-only error score.
Finite cohort completion alone is not classified as steady-state stability.
Simulation Monte Carlo intervals do not cover model or measurement error.

After this pilot, a separate real-database characterization/holdout study is
needed before claiming that the semantics improve PostgreSQL, MinIO or
arbitrary StatefulSet predictions.
