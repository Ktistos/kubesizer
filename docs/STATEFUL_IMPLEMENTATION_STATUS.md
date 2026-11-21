# Stateful modeling branch status

Branch: `feature/stateful-performance-models`, based on `199eb2b`.
Local implementation and verification: 2026-11-16 and 2026-11-17.

## Completed

- A sourced inventory of 16 reference patterns, organized by mechanisms rather
  than a purported exhaustive count of database architectures.
- Per-source sequential/parallel/exclusive routing and all/quorum/background
  completion in the native C++ executor. Remaining branches keep consuming
  resources after early acknowledgement; late returns retain valid ownership.
- Finite staged workflows with separate stage/resource identities, physical
  queue reuse, per-stage demand and work units, continuations, explicit
  admission holding, timers, and overlapping local/remote branches. Stage
  work is projected onto shared resource load/moment equations for conditional
  analytical bounds. See [STAGED_RESOURCE_WORKFLOWS.md](STAGED_RESOURCE_WORKFLOWS.md).
- Fixed/bounded components in portable initial sizing, native configuration,
  calibration bounds and exact replica search.
- An analytical opt-in for conditional mean completion bounds. Uncertified
  point means are not substituted for quorum response. Upper-bound failure
  alone is inconclusive and cannot establish a proven minimum-cost plan.
- Finite topology comparison through `--stateful-candidates`, available with
  either analytical or simulation evaluation. Every candidate supplies its
  own topology/demand evidence and physical cost; external traffic/SLOs must
  agree. Evidence is hashed and checked for changes before and after scoring.
- Synthetic examples for replication, shard routing, scatter/gather, quorum,
  shared storage and erasure-like fan-out, plus one/two-follower comparisons.
- Optional periodic CPU quota with bounded parallel execution and independent
  budgets per replica. Classes and stages share their physical resource's
  budget. This is a simulation abstraction with explicit runtime parameters;
  analytical latency evaluation abstains. See [CPU_BANDWIDTH_MODEL.md](CPU_BANDWIDTH_MODEL.md).
- CLI version 0.6.0, native protocol 18, semantics v21; calibration identity
  includes source policies, staged execution/demand/resource mapping, analytical
  mode and replica constraints. Existing VM pilots retain their frozen v18
  snapshots; the local extension was not deployed into an active experiment.
- Generic non-GPU components: bounded admission, token buckets, failures,
  bounded retries without implicit cancellation, all-work job completion,
  fixed-ratio or key-state caches, shared link/pool stages and fixed serverless
  pool lifecycle. Optional portable CSV/Prometheus-snapshot/OTLP/key evidence
  is typed, validated and hashed. Dynamic policies cause analytical abstention
  and do not impose unproved static traffic capacity floors. See
  [PORTABLE_COMPONENTS.md](PORTABLE_COMPONENTS.md).

## Verification

- Latest complete local suite, including MinIO readiness/measurement health
  and component campaign instrumentation: **1,026 passed**. Evidence:
  `results/component-pilot-cmp1-20261117/unit-tests.xml`.
- Prior component implementation suite: **983 passed**, with the result retained in
  `results/component-software-verification-20261117/unit-tests.xml`.
- Focused component, CPU-bandwidth, staged and native-backend checks against
  ASan/UBSan: **149 passed**. Release and sanitizer binaries build successfully;
  formatting and patch-whitespace checks passed.
- Component integration checks completed calibration, exact search and fresh
  validation for five synthetic examples. The fixed external API example
  correctly returned no reliable plan when its failure budget could not be
  certified. These are software checks, not new deployment results. See
  [the retained verification report](../results/component-software-verification-20261117/README.md).

- Full local suite for the CPU-quota change: **886 passed**.
- Focused CPU-quota, PostgreSQL, staged-workflow, occupancy and stateful checks
  against the ASan/UBSan backend: **155 passed**.
- Both new synthetic staged examples passed analytical CLI sizing with
  conditional bounds. The staged-overlap example completed reduced software
  calibration, exact sizing and **12 fresh final replications** with a
  `VALIDATED_FEASIBLE` model classification. This verifies software integration,
  not database prediction accuracy. Evidence is under
  `results/staged-workflow-software-check-20261116/`.
- A quota-enabled synthetic workload completed calibration, exact sizing and
  12 fresh final replications under protocol 17, classified
  `VALIDATED_FEASIBLE`. Reduced software-check settings and evidence are under
  `results/postgres-read-audit-pgp1-20261117/software-check/`; this is not
  independent deployment validation.
- The report-export smoke exposed NumPy scalar flags that were not converted
  for JSON. Scalar normalization and a regression test now cover that path.
- Formatting and patch-whitespace checks passed.
- The initial pre-stage software smoke test used synthetic `analytical-quorum.yaml`
  input in simulation mode, with reduced software-test calibration settings.
  Calibration, exact sizing and 12 fresh final replications completed;
  all three declared members stayed fixed at one. This is an integration
  check, not measured database performance or deployment reliability evidence.

## Limits and next experimental step

The comparison campaign is now manually held at the user's request.
The 18 November MinIO repair is `mio1-r7`: it moves S3 load off API-server
port-forwarding, preserves partial telemetry errors and retains the original
frozen rates/order. Ingress-boundary targets are fresh and separate from the
two completed older targets. See
[the repair record](../results/minio-pilot-mio1-r7-20261118/README.md).
The component campaign failed on its separate scheduling bug and remains
stopped; this MinIO repair does not restart it. Earlier launch notes below
describe the previous attempt sequence.
MinIO `mio1-r5` repairs storage readiness and separates measurement validity
from SLO outcomes, preserving receipt-verified characterization from `mio1-r4`.
The autonomous component pilot `cmp1-r2` is queued behind MinIO cleanup.
See [the MinIO repair record](../results/minio-pilot-mio1-r5-20261117/README.md)
and [the component protocol](COMPONENT_PILOT_CMP1.md). Their live status files
determine progress; neither controller restarts the comparison campaign.

The initial implementation produced no real database/storage accuracy result
and launched no live experiment. The subsequent user-authorized `sfp1` pilot
was instrumented on Okeanos on 2026-11-16, with a bounded pause/resume controller
for the existing comparison service. It tests a controlled persistent
application rather than a product database protocol. See
[STATEFUL_PRELIMINARY_PILOT.md](STATEFUL_PRELIMINARY_PILOT.md) and its live
`status.json` before making any collection or accuracy claim. The comparison's
frozen implementation, input/model declarations and completed evidence remain
unchanged.

The subsequent `sfp1` pilot completed all 18 targets and resumed comparison
collection. Its exploratory write improvement and weaker read predictions are
retained with a characterization/target demand-drift audit; no target-derived
correction replaced the frozen model. The next authorized real-PostgreSQL
pilot is defined in [POSTGRES_PRELIMINARY_PILOT.md](POSTGRES_PRELIMINARY_PILOT.md)
and has its own `pgp1` lifecycle controller. Consult that pilot's live status
before describing its targets or predictions as completed results.

MinIO `mio1` was subsequently queued behind `pgp1` at the user's request.
It collects real distributed S3 measurements and storage-call evidence. The
initial MinIO stage deliberately records analytical/simulation accuracy
abstention because a one-round quorum does not identify the product's complete
metadata/data protocol. See [MINIO_PRELIMINARY_PILOT.md](MINIO_PRELIMINARY_PILOT.md)
for the sequence, measurements and limits. A queued job is not a completed
MinIO experiment or an accuracy claim.

`pgp1` subsequently completed all 18 targets. Its original enriched model
had eligible mean-latency MAPE of 4.79% overall, 8.48% for reads and 1.09% for
writes. The six highest-load runs failed client health gates and remain
excluded; analytical evaluation abstained. The queued MinIO `mio1` attempt
failed during the comparison handoff before collecting measurements.

A subsequent retrospective CPU-quota replay reduced eligible read MAPE to
2.83% and overall MAPE to 1.81%, retaining the original CPU means, residuals
and eligibility. These are six read class/cells from two repetitions each,
not broad validation. Parallelism was an explicit node-capacity upper bound,
with sensitivity retained. Severe high-load underprediction remains; one
excluded read cell would falsely clear the point-mean SLO. The change is
therefore not a near-capacity safety result. See
`results/postgres-read-audit-pgp1-20261117/README.md` before interpreting this
as an improvement. The original experiment snapshots and evidence are retained.

The MinIO retry `mio1-r2` was launched on 17 November (Europe/Athens) with
bounded startup/manifest retries and trace-health checks. Its workload protocol
is retained. All 33 pilot/handoff tests and the server-side deployment dry run
passed. Its controller waits for a comparison completion receipt, collects
independently, cleans up and resumes comparison. Consult
`results/minio-pilot-mio1-r2-20261117/README.md` and live status before claiming
measurement collection or completion. Model-accuracy evaluation remains
pending execution-phase mapping.

The `mio1-r2` attempt subsequently failed on incompatible `mc admin trace`
flags after one healthy GET characterization repetition. `mio1-r3` repairs
those flags and multiline JSON decoding, and adds a live storage-trace
preflight. The pinned-image local integration check captured 139 real storage
records and verified PUT/GET contents; the full local suite passed 900 tests.
The new controller launched at 10:57 UTC on 17 November and waits for a
comparison completion receipt before collecting. See
`results/minio-pilot-mio1-r3-20261117/README.md` and its live status. Neither
this software check nor the single partial cluster repetition establishes
MinIO prediction accuracy or sizing reliability.

Architecture names do not select hard-coded database formulas. Database and
version can appear in provenance, while roles, routing, acknowledgement rules
and measured resource demand determine the model. Valid storage layout changes
must be resolved externally before comparison; node placement remains outside
scope.

Generic Istio/CPU telemetry does not identify replication replay, fragments,
fsync or multi-round commit. New per-source policies currently use explicitly
characterized frozen inputs. Elections, correctness/consistency protocols,
conflicts, state migration, healing, transactions and checkpoint barriers
are not implemented as product protocols. See the inventory for precise gaps.

Next: follow [STATEFUL_EXPERIMENT_PROTOCOL.md](STATEFUL_EXPERIMENT_PROTOCOL.md)
to collect characterization evidence for selected real configurations, freeze
the models and then measure untouched targets. Compare accuracy, abstention,
unsafe decisions, background stability and physical cost for both evaluators.
