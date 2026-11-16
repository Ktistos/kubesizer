# Synthetic stateful mechanism examples

All rates, service demands, pool limits and costs here are invented teaching
parameters. These are not STELAR, PostgreSQL, MinIO or other product profiles.
No new deployment experiments or accuracy results are included.

`primary-follower-one.yaml` and `primary-follower.yaml` describe separate,
resolved layouts. The first sends all reads to one follower; the second splits
them 70/30. Every follower also receives write propagation work. Both classes
contend for the same follower CPU/disk queues. Primary demand includes a toy
local cost of .002 seconds plus .0003 seconds per follower. This illustrative
relation is declared explicitly, not inferred from a database name.

`candidates.yaml` compares their total physical fixed cost plus searchable API
replica cost. Its evidence source is this description of synthetic parameters.
Replace that reference with retained real characterization files before using
the example as a deployment study. Changing the layout while silently keeping
incompatible demand measurements is not a valid scaling prediction.

`primary-follower-quorum.yaml` uses early follower acknowledgement instead of
background propagation. `sharded-replicated.yaml` routes to exactly one shard
and one eligible copy. `scatter-gather.yaml` visits both shards but selects only
one copy per shard. `leaderless-quorum.yaml` sends to all peers and waits for
two. `shared-storage.yaml` keeps one device shared by two compute services.
`multi-leader-origins.yaml` uses separate origin classes to avoid endlessly
echoing replication between leaders; it does not model conflicts.

`minio-like-erasure.yaml` is a toy four-member encoding/fan-out/disk graph with
an explicitly declared three-response rule. It neither discovers a MinIO
layout nor implements object placement, full commit phases, healing or pool
expansion. `analytical-quorum.yaml` demonstrates opt-in conditional mean
bounds with analytically supported local queues.

Use the existing CLI with `--workload FILE --replica-evaluation simulation`,
or `--stateful-candidates candidates.yaml --replica-evaluation simulation`.
For analytical bounds use `analytical-quorum.yaml` and
`--replica-evaluation analytical`. Many other examples intentionally have
finite synchronous holding and should abstain under analytical evaluation.

See ../../docs/STATEFUL_SCALING.md for input, scaling and uncertainty semantics.
