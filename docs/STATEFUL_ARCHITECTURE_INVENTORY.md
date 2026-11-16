# Stateful architectures and their performance mechanisms

Research and implementation inventory, 2026-11-16.

This replaces the earlier informal “seven families” explanation. There is no
finite, authoritative list of all stateful architectures. Replication, data
placement, commit protocols, and storage layout are separate dimensions. This
inventory covers common database, storage, messaging, and application-state
patterns relevant to Kubernetes capacity planning. It is not a claim that
Kubesizer implements or has experimentally validated those products.

## Dimensions to record

| Dimension | Choices that affect performance |
|---|---|
| Data ownership | One owner; hash/range/tenant partitions; shared storage; metadata/data separation |
| Replica roles | Standalone; passive standby; primary/follower; multiple writers; leaderless peers |
| Propagation | Full copies, log shipping, logical updates, erasure-coded fragments; direct or cascading replication |
| Response boundary | Local completion; named acknowledgement path; all branches; k-of-n responses; asynchronous propagation |
| Request routing | One data owner; eligible read replica; multi-partition scatter/gather; sticky entity/tenant; locality-sensitive routing |
| Resource demand | CPU, disk service, network transfer, admission/connection pools, shared storage contention |
| Time-dependent work | Repair, compaction, checkpointing, snapshots, resharding, catch-up, rebalancing |
| Coordination | Leader election; ordering; conflict resolution; locks; multi-round transactions |

Choices are not freely interchangeable. A declared quorum must match the
product protocol. A valid durability quorum is not necessarily a performance
model that “returns as soon as k arbitrary workers finish.”

## Reference patterns

“Representable” below means an explicitly declared healthy-operation queueing
abstraction. It does not mean a product adapter, measured input inference, or a
validated accuracy result. Fixed topology means the membership/data placement
is supplied before evaluation.

| Pattern | Sources and examples | Representation / remaining limitation |
|---|---|---|
| Standalone persistent server | [PostgreSQL HA overview][pg-ha] | Fixed service and optional explicit storage queue; extrapolation still needs resource evidence. |
| Active/passive, shared-disk failover | [PostgreSQL solution comparison][pg-solutions] | Active server and shared storage can be modeled; failover and fencing are not simulated. |
| Primary/follower with asynchronous propagation | [PostgreSQL standby operation][pg-standby] | Primary and followers are separate resources; background calls account for declared propagation work. No WAL batching or lag-consistency model. |
| Primary/follower with synchronous acknowledgements | [PostgreSQL replication configuration][pg-config] | Declared sequential/parallel phases and k-of-n completion; count only the follower responses named by the model. No election or consistency certification. |
| Cascading or geographical replication | [PostgreSQL standby operation][pg-standby] | A directed, finite propagation graph with per-source completion modes; link-specific bandwidth and failure domains need explicit resource models. |
| Leader-based consensus/log replication | [etcd performance][etcd], [RabbitMQ quorum queues][rabbit] | Healthy-state fan-out/acknowledgement abstraction only. Raft terms, elections, ordered commit, retries, fsync batching and failures are not implemented. |
| Multi-leader/active-active | [CouchDB conflicts][couch] | Separate origin request classes can declare finite propagation paths. Conflict detection, retries and reconciliation are not inferred or executed. |
| Leaderless/tunable-consistency replication | [Cassandra Dynamo architecture][cassandra] | Declared target fan-out and response count; background work continues. Read repair, speculative retry, and reconciliation require additional models. |
| Sharded/partitioned storage with replication inside each shard | [MongoDB cluster components][mongo], [Redis Cluster][redis] | Explicit shard routing, fixed shard members and separate class demands. No automatic shard-key distribution, repartitioning or transaction semantics. |
| Search scatter/gather over shard copies | [Elasticsearch read/write paths][elastic] | Parallel fan-out across shards, exclusive choice within a shard's eligible copies. Multi-round query/fetch using the same resources is not a built-in protocol. |
| Erasure-coded object/block storage | [MinIO design][minio-design], [MinIO erasure coding][minio-ec], [Ceph introduction][ceph] | Explicit coordinator/encoding/member/disk queues plus declared completion threshold. Not a MinIO/Ceph protocol implementation; fragment size, placement, data protection and expansion legality remain external facts. |
| Separate compute and shared/distributed storage | [Aurora architecture][aurora], [FoundationDB paper][fdb] | Several compute services can call the same storage resource and contend there. No automatic storage-tier cost/capacity model or transaction coordinator. |
| Metadata and data services / distributed filesystem | [Ceph architecture paper][ceph-paper] | Separate service paths for metadata and data. Cache coherence, leases, locks and metadata rebalancing remain unsupported. |
| Partitioned durable logs, queues and streams | [Kafka design][kafka], [RabbitMQ streams][streams] | Fixed partition leaders/followers and declared traffic can be represented. Offsets, consumer-group rebalances, retained-log capacity and backpressure protocols are not modeled. |
| Persistent keyed actors / sticky state | [Akka cluster sharding][akka] | Explicit data-owner routes can approximate stationary traffic. Entity ownership changes, activation and per-key mailbox serialization need a dedicated adapter. |
| Stateful stream processing and checkpoints | [Flink state][flink] | Measured processing or background resource demand can be declared. Checkpoint barriers, exactly-once recovery and state redistribution are not implemented. |

Tiered caches, materialized views, event sourcing, multi-region deployments,
and transactional engines combine these dimensions rather than form a final
additional set of mutually exclusive families. Multi-round commit, locking,
retry storms, failures, and data migration remain explicit gaps. This branch
must not be described as support for arbitrary StatefulSets.

## What this branch implements

1. Source-specific sequential, parallel, or exclusive-choice calls.
2. All-response, k-of-n response, and background completion. Unacknowledged
   branches continue to use resource capacity after the caller responds.
3. Existing shared service queues and request-class-specific demand for CPU,
   disk, or other deliberately defined processing resources. Calling a service
   `disk` does not infer its demand or create an I/O model from CPU telemetry.
4. Fixed/minimum/maximum replica constraints throughout initial sizing,
   calibration, search and direct system configuration.
5. Calibration fingerprints include call policies and constraints. The native
   protocol and semantics versions change; old calibration cannot be reused.
6. Analytical evaluation supports explicit opt-in conditional completion
   bounds. Strict point-prediction mode rejects per-source policies; finite
   parent holding and unsupported local queues still abstain. Expected traffic
   and necessary work-stability floors include background and late quorum work.
7. Finite topology-candidate comparison runs either evaluator on separately
   resolved layouts, accounts for physical fixed/elastic costs, and retains
   unresolved alternatives instead of claiming universal scaling laws.

See [the input and usage guide](STATEFUL_MODELING.md) and the
[experiment protocol](STATEFUL_EXPERIMENT_PROTOCOL.md). No live experiment was
run and no product-specific accuracy claim follows from the semantic tests.

## Primary sources

[pg-ha]: https://www.postgresql.org/docs/current/high-availability.html
[pg-solutions]: https://www.postgresql.org/docs/current/different-replication-solutions.html
[pg-standby]: https://www.postgresql.org/docs/current/warm-standby.html
[pg-config]: https://www.postgresql.org/docs/current/runtime-config-replication.html
[etcd]: https://etcd.io/docs/v3.6/op-guide/performance/
[rabbit]: https://www.rabbitmq.com/docs/quorum-queues
[couch]: https://docs.couchdb.org/en/stable/replication/conflicts.html
[cassandra]: https://cassandra.apache.org/doc/stable/cassandra/architecture/dynamo
[mongo]: https://www.mongodb.com/docs/manual/core/sharded-cluster-components/
[redis]: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
[elastic]: https://www.elastic.co/docs/deploy-manage/distributed-architecture/reading-and-writing-documents
[minio-design]: https://github.com/minio/minio/blob/master/docs/distributed/DESIGN.md
[minio-ec]: https://github.com/minio/docs/blob/main/source/operations/concepts/erasure-coding.rst
[ceph]: https://docs.ceph.com/en/latest/start/beginners-guide/
[aurora]: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.html
[fdb]: https://www.foundationdb.org/files/fdb-paper.pdf
[ceph-paper]: https://ceph.io/assets/pdfs/weil-ceph-osdi06.pdf
[kafka]: https://kafka.apache.org/41/design/design/
[streams]: https://www.rabbitmq.com/docs/streams
[akka]: https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html
[flink]: https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/
