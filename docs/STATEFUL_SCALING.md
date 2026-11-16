# Scaling depends on the distributed mechanism

This guide describes implemented configuration comparison and analytical
completion bounds. It does not report stateful experiment results.

## What changes when capacity is added

A database name cannot determine its scaling behavior: the same product can
be standalone, replicated, partitioned or remotely backed by shared storage.
Use the mechanisms, actual configuration and measured work. Product/version
can be recorded in evidence files without being a mandatory model selector.

| Mechanism | Potential capacity gain | Additional work or constraint |
|---|---|---|
| Passive standby | Availability, not ordinary traffic capacity | Standby synchronization and catch-up. |
| Primary + read followers | Eligible reads can spread across followers | Primary writes remain centralized; every follower needs updates. |
| Synchronous replication | Can spread some reads or improve availability | Commit waits for required acknowledgements; adding members can increase total work without improving write throughput. |
| Multi-writer / leaderless | Work may enter through multiple nodes | Replication, coordination, conflict/retry work and acknowledgement rules still constrain throughput. |
| Sharding | Different partitions can process work concurrently | Hot keys/shards, scatter/gather and cross-partition transactions can limit gains. |
| Erasure-coded storage | More valid storage groups can add capacity | Coding, fragment placement, quorum and device/network capacity matter; one more pod is not automatically a valid expansion. |
| Shared storage | More compute can process requests | The same storage layer can become the bottleneck. |
| Partitioned logs / keyed state | More independently assigned partitions can process work | Replication and partition ownership constrain useful concurrency; redistribution has a cost. |

These are mechanism-level expectations supported by the primary sources in
the [inventory](STATEFUL_ARCHITECTURE_INVENTORY.md), not empirical speedup laws.

For a primary with `n` read followers, one simplified declared workload is:

```
follower_i query rate = external_read_rate * read_routing_probability_i
follower_i replay work rate = external_write_rate * replay_work_per_write_i
primary CPU work rate = write_rate * (local_work + n * send_work_per_follower)
```

The latter expression is only appropriate if per-follower send work actually
follows that model. Batching, cascading and shared transfers can change it.
Replication arrival rates use meaningful units (transactions, batches or bytes),
not an assumed one-to-one equality with query counts. Assign read and replay
work to the same resource queues when they share hardware.

For erasure coding with `K` data and `M` parity fragments, `N=K+M`, an ideal
encoding produces approximately `B*N/K` fragment bytes for an object of `B`
bytes, excluding metadata and other overhead. This work expansion does not
predict disk time, parallelism, network topology or the product's commit
rule. Those facts and legal pool layouts must be supplied separately.

## Comparing resolved database configurations

The existing replica search scales interchangeable workers within one fixed
model. A change to storage membership must instead supply a new resolved
workload with updated routing, fan-out, demands and configuration evidence.
Both analytical and simulation evaluation use those complete candidate models.

```yaml
version: 1
candidates:
  - name: one-follower
    mechanism: primary with asynchronous follower and read offload
    workload: primary-follower-one.yaml
    base_cost: 3.0
    elastic_replica_costs: {api: 0.5}
    evidence_sources: [profile-one.yaml]
  - name: two-followers
    mechanism: primary with two asynchronous followers and read offload
    workload: primary-follower.yaml
    base_cost: 4.0
    elastic_replica_costs: {api: 0.5}
    evidence_sources: [profile-two.yaml]
```

`base_cost` is the full fixed physical configuration cost, in consistent units.
`elastic_replica_costs` gives the positive cost of each searchable service
replica. Every remaining modeled resource must have fixed replica bounds.
CPU/disk queues representing one physical server do not each become another
physical-server cost: include the server once in `base_cost`.

Every alternative must have the same external arrival streams, request-class
definitions, latency boundaries and SLOs. Different internal demand profiles
are allowed, but their evidence files are required and hashed with the frozen
workload. A profile may express an independently justified transferable demand
model; simply copying one installation's service latency into every candidate
does not establish that transfer is valid. Evidence hashes establish identity,
not the validity of the measurements or a proof of scaling.

```bash
kubesizer --stateful-candidates candidates.yaml \
  --replica-evaluation simulation --json
```

The planner evaluates all alternatives, runs the selected ordinary exact
replica workflow inside each, then compares `base_cost + elastic replica cost`.
Each simulation alternative gets its own calibration and fresh final validation.
The default Monte Carlo alpha budget is divided by the declared candidate
count. This does not bound model error or measurement bias. Failed final
validation never becomes an accepted candidate. Unsupported alternatives stay
visible as `UNRESOLVED`; they prevent a proven minimum-cost claim. If none is
accepted, the planner abstains. Minimum-cost claims, when available, apply only
to the finite declared models and cost units, not every possible database layout.

## Analytical completion bounds

Declare `analytical_call_model: response_bounds` at workload top level to
enable the new analytical mode, then run:

```bash
kubesizer --workload examples/stateful/analytical-quorum.yaml \
  --replica-evaluation analytical --json
```

For child response durations `X_1...X_n` with nonnegative values and supplied
mean bounds, no independence assumption is needed for these inequalities:

- Sequential: the mean of the sum is the sum of the means.
- Exclusive choice: the mean is the probability-weighted branch mean.
- All-response parallel calls: the expected maximum lies between the largest
  marginal mean and the sum of the marginal means.
- k-of-n completion: `E[X_(k)] <= sum(E[X_i]) / (n-k+1)`. Also
  `E[X_(k)] <= sum(the k smallest marginal means)`. The smaller valid upper
  bound is used. For k=n, the largest marginal mean supplies a lower bound;
  otherwise the generic lower bound is zero.
- Background: child work contributes zero to this caller's waiting time,
  while remaining present in the resource-load equations.

These inequalities are valid **conditional on the supplied marginal means**.
Those means come from Kubesizer's existing queueing model, which can itself be
an approximation. Consequently, the final result is not an unconditional bound
on a real deployment. It is a conservative completion calculation inside the
declared analytical model, with potentially substantial overprovisioning.

The output evidence is `ANALYTICAL_MEAN_LATENCY_UPPER_BOUND`, with
`request_class_mean_latency_upper_bounds`; the point-prediction field remains
empty. Mean SLO checks use the upper bound. A low actual latency can therefore
remain uncertified. If only the upper bound exceeds the SLO, classification is
`INCONCLUSIVE`; only a lower bound exceeding the SLO establishes a conditional
violation. Such unresolved lower-cost configurations also prevent a proven
minimum-cost claim. Report abstention separately from prediction error.

The analytical path still rejects finite synchronous parent holding,
non-CPU admission blocking, unsupported execution/dispatch assumptions,
percentile SLOs and deadlines. For a pure external entry, Poisson arrivals are
recognized even when its response spawns child work; downstream call streams
are not automatically labeled Poisson. The bounds require finite acyclic
call graphs and use the same declared local demands as simulation.

The mechanism implementation and these mathematical checks are ready for
prospective evaluation. They do not establish that either method predicts
database scaling accurately; that is the next experimental question.
