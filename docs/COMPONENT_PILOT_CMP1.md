# cmp1: controlled non-GPU component mechanism pilot

Prepared 17 November 2026 on `feature/stateful-performance-models`.
This is an exploratory characterization/holdout study. It does not certify
production sizing, arbitrary applications, or named broker/cache/serverless
products. Its controller is queued after MinIO `mio1-r5` on Okeanos.

## Isolation and autonomous lifecycle

The user explicitly requested that comparison experiments remain stopped.
`kubesizer-simulator-comparison-targets.service` was stopped and a persistent
intent marker retained at
`/home/experiment/experiments/comparison-manual-hold.json` on the VM.
Completed comparison artifact bindings were snapshotted separately; incomplete
attempts are retained by the original comparison runner. Frozen comparison
implementations, declarations and completed evidence were not edited.

The component unit waits for `kubesizer-minio-pilot-mio1-r5.service` to reach
a terminal state, finish its cleanup and remove all its pods. A failed MinIO
predecessor is recorded as failed, not as successful validation. It then
checks the explicit comparison hold and absence of active comparison replicas.
Only then can it deploy `kubesizer-components-cmp1/component-0`.

The campaign has its own namespace, namespace-scoped Role, source snapshot,
native binary/runtime libraries, protocol, seeds and result directory. It
uses no new PVC and does not alter STELAR resources. Cleanup scales only its
owned StatefulSet to zero. Neither MinIO nor this controller restarts the
comparison campaign. A systemd stop hook repeats cleanup after failure; a
12-hour watchdog bounds the wait and work. Collection does not depend on this
conversation staying open. Host reboot/disruption is not automatically retried.

## What is deployed

A real HTTP server and real concurrent threads implement six controlled
mechanisms. This deliberately exposes individual occupied-worker and delay
phases, admission state, failures and cache accesses. It supplies more evidence
than aggregate CPU/HTTP counters alone. Those observations can be exposed as
instrumented spans/counters, but cannot be inferred for every opaque product.

| Family | Controlled mechanism | Capacity settings | Omitted-mechanism ablation |
|---|---|---|---|
| Broker workers | Publisher acknowledgement plus job completion; per-worker FIFO processing, four admitted jobs, eight pending jobs and a 200 ms pending deadline | One/two logical workers | Unbounded pending admission, retaining the same job-completion boundary |
| Gateway/connection pool | Requests hold one finite pool slot over an independent downstream delay; bounded admission/deadline | Two/four pool slots | Independent delay without pool contention |
| Cache | Shared strict LRU, one-second TTL, 16 keys with declared IID popularity, conditional backend work and four backend slots | Four/eight 1 KiB payload slots | Fixed hit ratio measured during characterization |
| Shared link | One real sender thread serves 4 KiB chunks round robin; payloads are 64/256 KiB with equal probability | 1/2 MiB/s declared pacing | Independent transfer delay without shared contention |
| External API | Two-token burst, reject quota, four active attempts, local failures, at most three attempts with 60 ms deadlines and 5 ms backoff | 20/40 admitted attempts/s | Quota omitted; failures/retries otherwise retained |
| Serverless pool | Fixed logical instances, two admitted requests each, 150 ms startup, 200 ms idle expiry, pending limit/deadline | One/two logical instances | Warm instances without cold/idle lifecycle |

These are resource settings inside one experimental pod, not a demonstration
of Kubernetes replica placement or separate physical-server scaling. Worker
phase occupancy is measured directly; sleeping while holding a logical worker
is not reported as CPU demand. The paced sender is an application scheduler,
not a measured physical network or TCP congestion-control model. Its discrete
chunks are approximated by the model's continuous processor sharing. Cache
capacity is logical payload capacity, not process resident memory. The fixture
does not implement RabbitMQ, Redis, Knative or their full protocols.

## Characterization, inputs and freezing

There is one external request class per family. Each family/capacity setting
has two separate 90-second characterization windows after 15-second warmup.
The rate is at most four requests/s and at most 10% of the declared reference.
At least 50 measured samples are required for every needed occupied/delay phase
and the client-to-entry boundary residual. Phases exclude preceding queue
waiting; timed-out old attempts retain their work and their observations.
Reset is refused until both foreground and background work have drained.

The collector exports phase samples into the versioned optional component
CSV contract. Models reference typed `component_measurements` estimates and
consume the same parser used by offline workload loading. Source content
hashes, units, UTC windows and boundaries are retained. Per-phase distributions
are empirical IID samples. They are not fitted from variance between windows.
Cold-start/retry policies and key-generator probabilities are explicit fixture
declarations; they are not target-derived fits or claimed CPU-only inferences.

Both ablations use the same characterization samples, residual, workload and
configuration. The enriched variant changes the named mechanism only. This
is an ablation within the current engine, not a comparison with every feature
of an older release or with other simulators.

Before any target is collected, every model and its predictions are written
and hashed. Implementation identities and source inputs are checked again
before targets. Simulation predictions use 20 separate simulation seeds.
Analytical evaluation runs through strict compatibility checks and reports
conditional bounds where supported; otherwise it explicitly abstains.
No incompatible analytical point mean is substituted for missing coverage.

## Target design and timing

| Factor | Values |
|---|---|
| Families | Six |
| Resource settings | Two per family |
| Offered rates | 0.25, 0.65 and 0.95 times a characterization-based reference |
| Arrivals | Poisson; bursty renewal gamma with shape 0.25, SCV 4 |
| Physical run repetitions | Two per cell |
| Target warmup / measurement | 15 / 45 seconds |
| Cohort drain / cooldown | Up to 30 / 5 seconds |
| Total targets | 144 |

The reference is an occupied-worker, slot, byte-work or quota reference, not
observed CPU utilization or a proven saturation threshold. Cache reference
load assumes all misses; serverless reference omits cold overhead. Their
fractions must not be called measured 95% utilization. Bursty gamma renewal
arrivals do not represent every kind of burst correlation or arbitrary trace.

Characterization windows alone require 42 minutes. Target windows and nominal
cooldowns require 156 minutes. Draining, provisioning and prediction freezing
add time. Expected collection is **3.5–5 hours after MinIO cleanup**, with longer
cases bounded by the watchdog. This estimate is a schedule, not a result.

## Outcomes and limits

The report retains observed and predicted mean latency, signed error, absolute
percentage error, failures, analytical/simulation abstentions, eligibility and
point SLO decisions. The predeclared mean threshold is 250 ms and the terminal
failure threshold is 2%. These pilot point comparisons are not the standard
recommendation workflow's confidence-controlled feasibility certificates.

Latency-error summaries require at least 50 requests, complete/loss-free
evidence, no observed request failures and an adequate client scheduler.
Missing service boundaries and client-limited runs remain in raw results and
are excluded from model-error attribution. Failure outcomes remain visible
outside failure-free latency MAPE. A fast failed request cannot be presented
as a successful SLO decision.

Queue/admission trajectories and ongoing work are retained alongside latency.
Finite cohorts completing do not establish steady-state stability; bounded
queues can remain finite while rejecting too much work. Two physical repeats
do not establish reliable tail behavior or a low unsafe-decision probability.
Simulation variability does not cover model or measurement uncertainty.
This single-cluster mechanism pilot is followed by product-specific and
cross-cluster studies before general reliability claims.

## Inspecting progress

VM: `root@168.119.120.59`, unit `kubesizer-components-cmp1.service`.
Active immutable release: `/opt/kubesizer/component-pilot/cmp1-r2`.
Outputs: `/home/experiment/experiments/cmp1-r2/data`.

Read `status.json`, then `predictions-frozen.json`, `prediction-receipt.json`,
`target-results.json`, `scores.json` and `REPORT.md` when present. Raw request
traces, phase observations, occupancy and per-run receipts are under `runs/`.
The earlier `cmp1` controller was stopped while queued, before any collection,
to finalize client-boundary validation. Its source/status remain separate.
