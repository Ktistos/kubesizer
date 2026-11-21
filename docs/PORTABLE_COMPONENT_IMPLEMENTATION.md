# Portable component extensions: implementation record

Authorized scope (17 November 2026): implement the non-GPU mechanisms discussed
for messaging/worker pools, gateways/connection pools, caches, shared network
links, external API limits/retries, and fixed serverless instance lifecycles.
No new cluster experiment is launched by this implementation task. Existing
VM campaign snapshots and raw evidence remain separate.

## Design and completion checklist

- [x] Resource policies: bounded FIFO admission queues with rejection/timeouts,
  per-replica token buckets (wait/reject), explicit failures, cold start and
  idle expiry. Delay resources represent fixed exogenous response time without
  pretending that it is local CPU demand.
- [x] Staged retry controls with bounded attempts/backoff and attempt deadlines;
  timed-out work continues consuming resources and late results retain valid
  ownership. Failure propagation distinguishes all/quorum/background outcomes.
- [x] Explicit client-response versus all-work completion boundaries for jobs.
- [x] Shared cache state: fixed-hit-ratio scope and measured-key LRU/TTL behavior,
  miss fill and invalidation. State/configuration changes must be evidenced;
  cache lookup CPU is a separate declared resource phase.
- [x] Network, pool, broker and external-service examples compose shared resources
  and execution stages. Transfer work uses bytes and declared/measured rates;
  transport latency is kept separate from queue waiting.
- [x] Optional portable component measurement contract/compiler. Versioned metric
  mappings normalize observations and configuration; missing or ambiguous
  evidence fails closed. No unused measurements become mandatory fields.
- [x] Strict analytical compatibility and honest load projections. Conditional
  mechanisms cannot silently use static visit ratios as proven necessary
  capacity floors. Simulation checks failure budgets and fresh replications.
- [x] Native protocol/calibration identities include all new semantics.
- [x] Deterministic timing/failure/resource tests, sanitizer checks, full suite,
  end-to-end synthetic workflow verification, examples and user documentation.

## Implementation notes

Reuse the native C++ event kernel; no alternate simulator backend is added.
Resource policies belong to service specifications. Retry/cache branch controls
belong to staged workflows. Resources retain one identity across classes/phases.
Each externally visible measurement states its completion boundary and provenance.

Portable inputs may come from Prometheus, Istio, standard component exporters,
OpenTelemetry, or configuration. Equivalent definitions can use different raw
metric names. Aggregate CPU/response counters do not identify arbitrary internal
phases or cache reuse; such optional models require suitable evidence or a
restricted scope. A configured fixed hit ratio does not justify cache-memory
extrapolation. An external API's capacity is fixed unless an explicit legal
candidate changes it. Node placement and GPU modeling remain outside scope.

The serverless primitive is a fixed declared instance pool with startup delay,
concurrency and idle expiry. It does not claim to reproduce arbitrary HPA/KPA
controller policies, provisioning failure, or billing. Product-specific broker
consistency, cache coherence, TCP congestion control and failure recovery are
not inferred from product names.

Cluster accuracy evidence will be collected after implementation. Synthetic
software verification is not reported as deployment prediction accuracy.

## Completed implementation and evidence

The native resource/cache/retry/lifecycle implementation uses protocol 18 and
semantics v21. Measurement compilation lives in
`performance_models/component_measurements.py`; validated policies and
simulation eligibility live in `performance_models/components.py`. All changes
use the existing statistical calibration, exact search and final validation.

See [the component guide](PORTABLE_COMPONENTS.md) for the contract and declared
limits, and [the software verification report](../results/component-software-verification-20261117/README.md)
for retained integration results. Dynamic mechanisms cause analytical abstention
rather than substituting an uncertified approximation. No new real-deployment
experiment was launched and no VM campaign snapshot was updated.
