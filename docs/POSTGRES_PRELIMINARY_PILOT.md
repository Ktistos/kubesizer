# PostgreSQL pilot pgp1

This protocol was prepared on 2026-11-16 before collecting PostgreSQL target
data. It follows the controlled `sfp1` audit; it is not a completed result.

## Question and scope

Does explicit role-aware routing and local/one-standby/all-standby response
completion improve fixed-configuration latency predictions for an actual
PostgreSQL deployment? The scope is one primary and two physical streaming
standbys, a fixed dataset and a fixed mixture of read aggregation and row
updates. It does not establish database scaling, failover, consistency,
transactional performance for arbitrary queries, or general StatefulSet
reliability. Node placement optimization is outside the study.

The database image is pinned to
`postgres@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24`
(PostgreSQL 17.11, verified locally). Each member gets one fixed CPU limit,
its own 2 GiB Longhorn volume and a distinct Okeanos worker node. The database
uses real WAL, local fsync and physical streaming replication. The gateway
issues SQL; no synthetic CPU delay or fake database acknowledgement is added.

Reads go to exactly one standby, with probabilities 0.75 and 0.25. The read
operation hashes and aggregates a range of 4,096 stored rows. Writes update
a row version and part of its fixed-size payload, then commit. The external
mix is 75% reads and 25% writes.
These are restricted database operations, not a representative universal mix.

## Acknowledgements

| Label | Primary setting | Session commit setting |
|---|---|---|
| `async` | Empty `synchronous_standby_names` | `synchronous_commit=local` |
| `quorum1` | `ANY 1 (standby1, standby2)` | `synchronous_commit=remote_apply` |
| `all` | `ANY 2 (standby1, standby2)` | `synchronous_commit=remote_apply` |

Local mode still waits for local durable commit. `remote_apply` waits for the
selected standbys to replay the transaction, rather than merely receiving
bytes. Settings are verified at run boundaries and never switched during
active traffic. See PostgreSQL's [replication configuration](https://www.postgresql.org/docs/17/runtime-config-replication.html)
and [commit configuration](https://www.postgresql.org/docs/17/runtime-config-wal.html).

## Characterization and observability

Two low-load local-commit runs precede every target. Each has a 90-second
warmup and 240-second measurement window, at 12 requests per second. They
retain client request traces, SQL response timing, SQL backend PIDs, CPU
counters from the pod's PostgreSQL processes, replication LSN backlog and
wait-event snapshots. Process identities include start time to prevent
joining reused PIDs. A small sidecar shares the pod PID namespace; it does
not inspect host processes. Thus the measurement tooling is explicit and
reproducible, but goes beyond Istio request counters alone.

Class CPU demand is measured from backend CPU deltas divided by completed
operation counts. Standby startup-process CPU divided by writes estimates
replay CPU demand. The within-class CPU variance is **not identified**, so
the first diagnostic model uses deterministic CPU demand with that measured
mean. Auxiliary PostgreSQL process CPU remains visible rather than being
silently interpreted as per-query work. The model can miss that contention.

Low-load SQL wall time minus CPU demand gives a blocking/transport residual.
Its distribution is a diagnostic decomposition, not a uniquely identified
database execution phase. The client-to-gateway residual is measured
separately and retained identically by both model variants.

After those runs, isolated local commits are followed by a WAL flush marker
query and independent polling of each standby's replay LSN. Those witnesses
measure an **upper proxy** for remaining replay acknowledgement delay:
marker queries, transport and polling resolution are included. They do not
identify per-WAL-record disk service demand. If replay CPU cannot be resolved
or the witness cannot be separated from modeled CPU work, the procedure
records abstention instead of inventing parameters. No target latency is
used to recover missing inputs.

## Models and important mismatches

Both variants use the same CPU means, blocking samples, witness delays,
boundary residuals, resource counts and external rates. The legacy ablation
removes source call policies, retaining role-specific queues. The enriched
variant uses exclusive read routing and the actual acknowledgement rule.
Neither is described as the complete historical release.

Database CPUs are shared by read/replay demand; all propagated write work
remains in the offered-load equations after early acknowledgement. Standby
acknowledgement-delay centers represent the measured proxy, not extra
physical disks. Every declared physical or logical resource is fixed during
this prediction pilot. No candidate database layout is invented by search.

The current model orders local completion before child replication.
PostgreSQL can overlap local commit and replication. PostgreSQL also batches
WAL and replays it in order; independent model visits do not fully represent
those mechanisms. Shared Longhorn contention is not modeled independently.
These are predeclared potential errors, not hidden adjustments to be fitted
from holdout outcomes.

The analytical evaluator is attempted with its existing strict compatibility
checks. Unsupported blocking/holding produces abstention. Conditional bounds
are not substituted for point means in a latency-error score. Simulation
produces diagnostic means from 20 fresh replications; this is not the full
calibration/optimization/final-validation recommendation workflow.

## Targets, gates and reporting

Eighteen runs cover three modes, fractions 0.25/0.60/0.90 and two physical
repetitions. Each uses 60 seconds of warmup, 240 seconds of measurement, up to
90 seconds for cohort/replication drain and 15 seconds cooldown. Target order
is randomized and frozen. Models and all predictions are hashed before the
first target. The initial time budget is 3–5 hours including setup.

The CPU-demand capacity reference uses characterization only. The effective
anchor is capped at 200 requests per second for a bounded pilot. If that cap
binds, the high-load label must **not** be presented as near measured capacity.
Actual CPU, wait events, replication backlog and client scheduling delays are
retained. The mean-latency thresholds are 0.25 seconds for each class; these
are declared diagnostic thresholds, not production SLO recommendations.

Report mean-latency error and coverage per class/mode/load; retain failures,
timeouts, missing evidence and incomplete cohorts. Do not infer steady-state
stability from finite completion. Report point-mean SLO crossings with that
qualification; no unsafe-decision probability or safety certificate is
claimed from two repetitions. Inspect characterization/target demand drift
before interpreting accuracy changes. Do not tune phase assumptions on
these targets and then relabel the same outcomes as a fresh holdout.

## Isolation and lifecycle

Resources belong to `kubesizer-postgres-pilot-pgp1`. Existing STELAR databases
and storage are not reused or modified. A generated database password lives
in a namespace Secret, not source code or a report. The VM's existing
collector gets a Role restricted to this namespace; administrator credentials
are not copied to the VM.

After a completed comparison treatment and evidence receipt, the controller
pauses the comparison service, checks its application replicas are stopped,
then starts this pilot. Completed evidence and all frozen comparison
declarations remain unchanged. At success or failure, it scales down only
this pilot's members and gateway, waits for the pods to disappear, and resumes
the comparison. A systemd stop hook repeats cleanup after a process failure.
Persistent volumes remain for inspection; storage reclaim is not automatic.
