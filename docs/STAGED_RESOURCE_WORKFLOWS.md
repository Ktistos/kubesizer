# Execution stages over shared resources

Implemented on `feature/stateful-performance-models`, CLI 0.6.0; introduced in
protocol 16 / semantics v19 and retained in protocol 18 / semantics v21.
This is a generic performance-model mechanism,
not a certified implementation of every database or object-store protocol.

## The distinction

A **resource** owns capacity, replicas, admission and processing queues.
A **stage** specifies work on a resource and what happens afterward.
Different stages may use the same resource. They contend in that resource's
existing queue; naming another stage does not add a CPU, disk or replica.

For example, metadata, fragment and finalization operations can all reference
`disk_1`. The engine does not create three independent disks. Request classes
also share the same physical resource when they use the same resource name.

The existing routing/calls/workflow formats remain valid. A request class can
instead declare one `stage_workflow`. It cannot mix that field with the other
flow formats. Ordinary portable telemetry is not automatically decomposed
into these phases: staged inputs are explicitly characterized frozen models.

## Input

```yaml
analytical_call_model: response_bounds
request_classes:
  write:
    concurrency_weight: 1
    end_to_end_latency_slos: {mean_seconds: 0.1}
    stage_workflow:
      entries: {coordinator: prepare}
      stages:
        prepare:
          resource: coordinator
          service_time: {distribution: deterministic, mean: 0.002}
          next: propagate
        propagate:
          calls: {write_1: 1, write_2: 1}
          call_policy: {completion: quorum, required: 1}
          next: finalize
        write_1:
          resource: disk_1
          service_time: {distribution: exponential, rate: 200}
        write_2:
          resource: disk_2
          service_time: {distribution: exponential, rate: 100}
        finalize:
          resource: coordinator
          service_time: {distribution: deterministic, mean: 0.001}
traffic_model:
  entry_services:
    write:
      coordinator: {distribution: exponential, rate: 5}
  service_times:
    coordinator: {replica_queue_model: pooled}
    disk_1: {replica_queue_model: pooled}
    disk_2: {replica_queue_model: pooled}
replica_constraints:
  coordinator: {fixed: 1}
  disk_1: {fixed: 1}
  disk_2: {fixed: 1}
```

These numbers are illustrative. They are not measured MinIO/PostgreSQL inputs.
`traffic_model.service_times` declares the physical resources and their
execution/queue configuration. For staged classes, the work distributions
come from the stages. Explicit stage demand overrides resource-level fallback
distributions; an ordinary class sharing that resource retains its own
resource-level distribution.

`entries` maps each external entry resource to its initial stage. Its keys
must match that class's external streams. Several entry resources are allowed;
their rates determine the mixture of work. Each entry resource must be used
by at least one resource stage reachable from that entry. The initial stage
may itself be a control stage. Changing entry proportions after construction
requires reconstructing the workload so resource moment mixtures stay valid.
Uniformly scaling the streams preserves those mixtures.

## Stage fields

| Field | Meaning |
|---|---|
| `resource` | Existing shared resource; omit for a control stage. |
| `service_time` | Local work distribution; required for a resource stage. |
| `work_units` | Positive amount of work, multiplying sampled `service_time`; default 1. |
| `unit` | Provenance label, such as `byte`; default `work`. |
| `non_cpu_blocking_time` | Optional local wait after processing, holding admission but releasing processing capacity. |
| `hold_resource` | Whether admission remains occupied while child calls are awaited; default false. |
| `delay` | Optional timer on a resource-free control stage; consumes no capacity. |
| `calls` | Target **stage names** and positive expected call counts. |
| `call_policy` | Per-stage parallel/sequential/choice execution and all/quorum/background completion. |
| `next` | Local continuation stage after this stage's completion rule is satisfied. |

A resource stage performs local processing and optional blocking first.
Default behavior releases its resource before invoking children. With
`hold_resource: true`, processing capacity still releases after local work,
but one admission slot remains occupied until child completion. Background
completion never holds admission for its unawaited branches. Admission always
releases before `next`, allowing a later stage to reacquire the same queue.
Holding finite admission while recursively requesting it can deadlock; the
simulation must observe that behavior rather than invent spare capacity.
Analytical evaluation rejects declared synchronous holding.

Control stages have no service demand, replica or queue. They can wait on a
timer, fork children, join their responses and continue. This avoids an
artificial near-zero CPU service just to express a fork/join. Delays cannot
be placed directly on resource stages; use an explicit control stage after
the resource stage. Resource-wide `post_release_delay` is rejected for a
resource used by staged workflows to avoid ambiguous/double-counted delay.

`calls` use the existing modeled round-trip communication distribution.
`next` is a local workflow continuation and adds no implicit network delay.
An explicitly measured transition delay belongs in its own control stage.
Network bandwidth contention can be declared as another shared resource with
byte-scaled work; this does not add packet/TCP simulation.

## Quorums, overlap and late work

After the required number of distinct child-stage responses, a quorum stage
can enter `next`. Other branches remain active and continue to contend, even
if they subsequently use a resource needed by finalization. A background
stage enters `next` without waiting for children. Late return events retain
valid ownership; a client deadline does not cancel server work.

`required` counts responses from distinct declared branches. It does not
certify independent failure domains or data durability. A model author must
map those branches to actual members and acknowledgement boundaries.
Choice selects exactly one child with probabilities summing to one;
fractional parallel calls retain independent optional-call behavior.

Overlap is expressed by parallel composition. For example, a control stage
can wait for **both** a local CPU/flush chain and a remote quorum chain:

```yaml
start:
  calls: {local_cpu: 1, remote_quorum: 1}
  next: finalize
remote_quorum:
  calls: {apply_1: 1, apply_2: 1}
  call_policy: {completion: quorum, required: 1}
```

The local chain and replication chain start together. The parent all-response
join waits for local completion and the remote quorum. Counting local work
as just another member of a flat quorum would express a different rule.
See [staged-overlap.yaml](../examples/stateful/staged-overlap.yaml).

The finite stage graph is acyclic, while the physical resource graph may
revisit the same resources. Multiple requests, classes and simultaneous
instances of a stage share queues correctly. Inputs are limited to 256
stages and at most 10,000 possible stage visits per request. Unknown stages,
cycles, unreachable stages, invalid work scales and invalid quorums fail
before execution; the native parser independently checks the executable graph.

## Demand, scaling and analytical evidence

Supply work measured outside resource queue waiting. With a measured
seconds-per-byte distribution, `work_units` can be bytes processed in that
stage. An explicitly resolved erasure layout may assign fragment-byte work
to each member. The engine does not derive fragment sizes, parity or valid
expansion rules from a product name. Blocking/delay is not automatically
scaled by `work_units`; only processing demand is scaled.

For capacity calculations the parser projects expected stage visits and
first/second work moments onto each physical resource. All late/background
work is included. A moment-equivalent gamma/deterministic carrier is used
only by existing queueing/stability calculations. Native staged execution
samples the original per-stage distributions and scales them by work units;
it does not sample that aggregate carrier.

Analytical `response_bounds` recursively combines stage-local queue mean
estimates, completion inequalities and continuations. It uses the same
shared-resource load equations as simulation, rather than independent queues
per stage. Outputs remain **conditional mean bounds**, not point means or
unconditional deployment guarantees. Queue response means are approximate,
particularly for correlated reentrant traffic. Such arrivals are not silently
declared Poisson. Blocking, finite synchronous holding and other unsupported
queue assumptions still cause abstention. Upper-bound failure alone remains
inconclusive. Strict point-mean evaluation rejects staged workflows.

Physical replica constraints, exact search, calibration identity and fresh
final simulation validation apply as before. Stage demand, ordering, resource
mapping, units and policies are all included in calibration fingerprints.
Old protocol/calibration profiles cannot be reused for the new executor.

## Examples, use and experimental limits

```bash
native_backend/cpp/build.sh
kubesizer --workload examples/stateful/staged-storage.yaml \
  --replica-evaluation analytical --json
kubesizer --workload examples/stateful/staged-overlap.yaml \
  --replica-evaluation simulation --json
```

[staged-storage.yaml](../examples/stateful/staged-storage.yaml) demonstrates
metadata, encoding, fragment acknowledgement and finalization stages that
reuse the same disks. It is a synthetic mechanism example, **not** an adapter
or fitted model for MinIO. Correct product configuration and phase-level
characterization remain necessary. The extension alone establishes no
improvement on real MinIO/PostgreSQL latency or sizing.

The running PostgreSQL pilot and queued MinIO pilot keep their frozen code,
models and reporting rules. This local change does not overwrite their inputs
or retroactively label them as evaluations of the new executor. A subsequent
phase mapping must be frozen and validated on untouched target measurements.

Elections, failure recovery, data migration, lock managers, transactional
consistency, state-dependent retries, byte inventories and product-level
autoscaling/deployment legality are not supplied by these finite workflows.
Their absence must not be described as support for arbitrary stateful systems.
