# Declaring stateful performance models

This branch extends the existing queueing network and native event kernel. It
does not infer storage architecture from a Kubernetes controller kind or an
application image name. The [architecture inventory](STATEFUL_ARCHITECTURE_INVENTORY.md)
separates implemented mechanisms from product-specific gaps.

## Separate physical resources from operations

Use one named service for each modeled shared processing resource. Different
request classes that use the same resource must reference the same service,
with class-specific `service_times` where their work differs. Two classes
called `read` and `replication` must not receive independent copies of the same
physical CPU or disk capacity.

CPU and disk can be separate named queues connected by calls when the declared
operation order supports that decomposition. Disk service time must exclude
disk queue waiting. Network delay distributions represent transit delay, not
bandwidth contention; a shared bottleneck requires an explicit resource queue.
Repeated stages using the same resource with different continuation rules,
multi-round transactions, locks, and state-dependent retries need further
engine support. Splitting a shared device into independent stage resources is
not a faithful workaround.

## Call policies

The existing class-level `call_execution` remains the default. Optional
`call_policies` override one source within that class:

```yaml
request_classes:
  write:
    concurrency_weight: 1.0
    calls:
      api: {primary: 1.0}
      primary: {follower_1: 1.0, follower_2: 1.0}
    call_execution: sequential
    call_policies:
      primary:
        execution: parallel
        completion: quorum
        required: 1
    end_to_end_latency_slos: {mean_seconds: 0.100}
```

This primary performs its own declared work, starts both follower branches,
and responds after one follower branch returns. `required` counts child
responses; it does not count the primary itself. The other branch keeps
processing and can delay subsequent requests. The primary's admission slot is
released when its modeled response is ready. Additional descendants may still
hold their own slots. Explicit post-release response delay is applied once.

| Field | Accepted values | Meaning |
|---|---|---|
| `execution` | `parallel`, `sequential`, `choice` | Start all eligible children together, run them in order, or choose exactly one target. |
| `completion` | `all`, `quorum`, `background` | Wait for all child responses, wait for the declared number, or acknowledge after local work without waiting for children. |
| `required` | Positive integer; quorum only | Number of distinct child responses required. |

For quorum completion, each target must have exactly one call, and execution
must be parallel. Integer counts of repeated calls to one service are not
distinct quorum members. Background completion also requires parallel
execution. All branches run to completion; no cancellation policy is assumed.
Client deadlines still terminate client outcomes without cancelling server
work. Late work retains valid native ownership until its return events finish.

For exclusive routing, the outgoing values are probabilities summing to one:

```yaml
calls:
  read_router: {reader_1: 0.8, reader_2: 0.2}
call_execution: parallel
call_policies:
  read_router: {execution: choice}
```

Exactly one reader receives each visit. Without `choice`, the two fractional
call counts produce independent optional calls, which can select both or none.
These models have the same expected rates but different request paths.

For asynchronous propagation:

```yaml
call_policies:
  primary: {execution: parallel, completion: background}
```

The expected traffic equations count all propagated work in all cases.
Lower latency from early acknowledgement must not hide follower saturation.
The necessary stability floor includes the slowest unacknowledged member.
Calibration additionally examines queue trajectories. The measurement cohort
ends after its client outcomes terminate; background work may remain then.

## Replica constraints

```yaml
replica_constraints:
  primary: {fixed: 1}
  follower_1: {fixed: 1}
  follower_2: {fixed: 1}
  shared_disk: {fixed: 1}
  api: {minimum: 1, maximum: 8}
```

Place this mapping beside `request_classes` and `traffic_model` in a frozen
workload, or beside `services` and `request_classes` in a portable workload.
Each named physical member normally stays fixed at one. Unlisted services
retain the ordinary interchangeable-replica assumption; list every storage
member explicitly. Capacity cannot be rescued by silently duplicating a
primary, shard owner, shared volume or erasure-set member. A fixed component
that cannot meet the necessary capacity floor causes rejection.

Changing follower count, shard count or erasure layout requires a separately
resolved workload: update membership, routing, replication/fragment work and
the applicable service-demand measurements together. Ordinary replica search
does not invent that topology transformation or validate storage deployment
legality. Physical resource cost also remains a separate supplied fact when
one node is represented by several CPU/disk/network queues.

## Measurements and observability

The ordinary portable workflow can now preserve fixed component bounds. Its
existing response-time decomposition cannot identify arbitrary asynchronous
or quorum protocols. Therefore its parser deliberately rejects `call_policies`:
use a frozen workload with explicitly measured internal resource demands for
this experimental extension. Existing synchronous workflows continue to work.

An engineer must supply configuration/observations for:

- role and partition membership, read-routing probabilities and call counts;
- local work by operation, including propagation/encoding/replay work;
- the response acknowledgement rule and measured execution/admission limits;
- storage and network capacity where those resources constrain the workload;
- object-size classes, key popularity and data placement where relevant;
- background activity, or an explicitly restricted healthy steady-state domain.

Prometheus can collect application exporters as well as platform metrics.
Istio HTTP request counts alone do not reveal disk fragments, WAL replay,
fsync work, consistency settings, or storage membership. No new universally
available telemetry fields are claimed. Preserve the source and units for
every demand estimate; do not interpret aggregate window variation as a
per-request distribution.

## Running a model

Build the updated protocol before running the branch:

```bash
native_backend/cpp/build.sh
kubesizer --workload examples/stateful/primary-follower.yaml \
  --replica-evaluation simulation --json
```

The CLI keeps its existing interface. Simulation still performs calibration,
exact replica search within the bounds, and fresh-seed final validation.
Strict analytical point-prediction mode rejects explicit per-source call
policies. The optional `analytical_call_model: response_bounds` supports the
conditional completion bounds described in
[STATEFUL_SCALING.md](STATEFUL_SCALING.md). Those outputs are identified as
upper bounds, not point predictions. Finite parent holding and unsupported
local queue/admission semantics continue to abstain.
Analytical work/traffic diagnostics remain necessary capacity checks, not
proofs that a storage protocol is stable or its SLO is met.

The examples contain invented demonstration parameters, clearly marked in
their files. They are executable semantic examples, not STELAR/MinIO profiles
or measured performance results. The MinIO-like example represents one
declared erasure-operation abstraction, not the complete MinIO algorithm.

For comparisons across layouts, use `--stateful-candidates` with the same
`--replica-evaluation analytical|simulation` flag. Database/product name is
optional provenance; the required mechanism description documents the model
and does not select product-specific formulas. See the scaling guide for the
candidate manifest and evidence requirements.

## stelarctl integration boundary

The feature model can declare roles, replica bounds and a reference to a
measured workload. A separately resolved product/configuration provides the
membership and valid storage layout. Kubesizer sizes eligible application
workers around those members. This branch changes Kubesizer only; it does not
modify stelarctl, automatically create StatefulSets, or apply a deployment.
