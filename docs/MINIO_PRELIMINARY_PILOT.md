# MinIO mio1: queued distributed-storage measurements

**Current operational amendment (18 November):** `mio1-r7` uses ingress for
S3 load and keeps kubectl forwarding for telemetry/readiness only. All six
targets are fresh at the original frozen rates/order; old tunneled targets
remain separate. See [the repair record](../results/minio-pilot-mio1-r7-20261118/README.md)
and live status before describing completion. Snapshot gaps are retained and
disclosed; the comparison campaign remains manually stopped. The original
protocol below describes the initial attempts and their lifecycle.

Prepared on 2026-11-16 before MinIO target collection. The autonomous controller
waits for PostgreSQL `pgp1` to terminate, complete cleanup and stop its pods.
It records the PostgreSQL outcome, including failure if that occurred; a
failed predecessor is not reported as a successful database evaluation.
Only then does MinIO take a completed-run checkpoint from the comparison
campaign and pause comparison load. No PostgreSQL/MinIO load overlaps.

## Real layout and workload

The isolated namespace is `kubesizer-minio-pilot-mio1`. Four fixed members use
four separate worker nodes and one new 2 GiB Longhorn volume each. The image
is pinned to
`ghcr.io/coollabsio/minio@sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9`.
It is the locally available distribution of MinIO
`RELEASE.2025-10-15T17-29-55Z`, not an assertion that this build is the latest
official production distribution. The actual image IDs/version are retained.

There is one four-drive erasure set, configured `EC:2`. The runner verifies
the actual erasure backend, four online/zero offline drives, parity two,
one set and four drives per set before characterization. It does not create
four independent copies of standalone MinIO. Existing STELAR buckets and
storage are not used or modified.

Real S3 GET and PUT operations use deterministic incompressible payloads of
64 KiB and 1 MiB, defining four separate operation/size classes. Each class
has share 0.25 in mixed runs. A bounded keyspace of 64 keys per size limits
storage growth; PUTs overwrite those keys. GETs verify payload length and
SHA-256. The timing boundary is the VM S3 client, including payload
verification. All requests use the first member as a fixed entry. The
standard S3 SDK signs requests; no custom object-store protocol is invented.

Longhorn adds another shared, replicated storage layer. These results cannot
be presented as performance on dedicated local disks or as evidence for all
MinIO deployment layouts. Failure, healing, rebalance, pool expansion and
topology changes are outside this first pilot.

## Measurements and protocol coverage

The pilot retains client request traces, errors, schedule delays, per-member
process CPU/I/O counters, MinIO exporter metrics, actual layout/version and
storage-call traces from the bundled `mc admin trace`. The observer shares
only the pod's process namespace. Credentials stay in a namespace Secret;
stored traces omit header, authorization, signature and credential fields.
The S3 client dependencies are installed into a separate directory and
their wheel hashes/package versions are retained.

An important predeclared limitation: a MinIO GET/PUT includes metadata,
storage and finalization phases. Read/write quorum values are not a complete
description of the whole operation's response boundary. The current generic
one-round quorum example therefore cannot be labeled a faithful MinIO model.
See the pinned upstream [object operations](https://github.com/minio/minio/blob/RELEASE.2025-10-15T17-29-55Z/cmd/erasure-object.go)
and [metadata/quorum calculation](https://github.com/minio/minio/blob/RELEASE.2025-10-15T17-29-55Z/cmd/erasure-metadata.go).

This queued first stage collects **real measurements and evidence for mapping
those phases**. Both analytical and simulation accuracy/sizing evaluation
explicitly abstain until that mapping and its resource demands are supported.
Observed latency is reported as observed latency, not as prediction accuracy.
No fabricated per-drive service time, arbitrary completion count or target-fit
residual is inserted to force a favorable simulation comparison. This is
an observability/semantics coverage result even if it exposes a model gap.

## Matrix and separation

- Characterization: two isolated runs per operation/size class; eight runs.
  Each uses 15 seconds warmup, 90 seconds measurement, at two requests/s.
- Capacity/client pilots: mixed traffic at 4, 12, 24 and 48 requests/s, stopping
  at the first unhealthy run. Each uses 15 seconds warmup and 45 seconds
  measurement. Client scheduling, request failures and the declared 0.25 s
  mean threshold determine the highest healthy **tested** reference rate.
- Targets: fractions 0.25, 0.60 and 0.90 of that reference, two repeats each;
  six mixed runs. Each uses 60 seconds warmup, 240 seconds measurement,
  up to 90 seconds cohort drain and 15 seconds cooldown.

The reference and target order are frozen and hashed before targets. No
target outcome chooses the rate reference. Passing the highest pilot gives
only a lower bound on achievable throughput: these fractions must not be
called measured server utilization or near-capacity without further evidence.
Completed finite cohorts alone do not establish steady-state stability.

Provisioning, prepopulation, characterization, pilots and targets are expected
to take roughly **1.5–3 hours after PostgreSQL and its checkpoint handoff**.
The wall-clock watchdog allows for both the predecessor wait and collection.
Two target repeats are exploratory and do not establish reliable tails or a
low unsafe-decision probability.

## Lifecycle and outputs

The VM unit is `kubesizer-minio-pilot-mio1.service`; its code is isolated under
`/opt/kubesizer/stateful-pilot/mio1`. Status and evidence live under
`/home/experiment/experiments/mio1/data`. The controller is started while
queued, so it does not depend on this conversation remaining open.

After PostgreSQL cleanup it pauses comparison at a completed evidence receipt,
deploys only its own resources, collects data and writes `REPORT.md` plus
`model-coverage.json`. Failure/incomplete outcomes remain in run artifacts.
At success or failure it scales down its four members, waits for its pods to
disappear, then resumes the comparison service. A systemd stop hook repeats
cleanup after unexpected process termination. Completed comparison evidence
and frozen comparison declarations are not changed. Test PVCs remain for
inspection and are not automatically deleted.

If fitting or a protocol change is later chosen using these observations,
its performance must be checked against fresh untouched targets; retrospective
fitting is not relabeled as an independent holdout result.
