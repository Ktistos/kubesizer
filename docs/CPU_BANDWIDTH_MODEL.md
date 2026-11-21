# Periodic CPU quota in simulation

This optional model was added after inspecting the `pgp1` read discrepancy.
It was introduced in native protocol 17 / semantics v20 and is retained in
protocol 18 / semantics v21. It is not enabled by
default and does not change the earlier frozen PostgreSQL or comparison runs.

## Reason for the extension

Continuous processor sharing spreads a replica's CPU capacity smoothly over
time. A Kubernetes CPU limit instead commonly enforces a CPU-time quota over
a period. Several runnable processes can execute on different physical CPUs,
consume the quota early, and then wait for replenishment. A limit of one CPU
does not itself pin all processes to one physical core.

The [Linux CFS bandwidth documentation](https://docs.kernel.org/scheduler/sched-bwc.html)
describes this quota/period mechanism and its implementation caveats. This
extension models a periodically replenished budget and bounded parallel
execution. It does not reproduce the complete Linux scheduler, per-CPU quota
silos, scheduling slices, CPU frequency changes, hierarchical limits, or
competition from processes absent from the workload.

Historical Prometheus data for `pgp1` records quota and period both equal to
100,000 microseconds. The busy standby was throttled in about 26% of periods
in one medium-load run and 100% in one high-load run. These observations
support testing quota semantics; they do not identify every cause of latency.
Raw queries and responses are retained under
`results/postgres-read-audit-pgp1-20261117/`.

## Frozen-model declaration

```yaml
traffic_model:
  # entry_services and other services omitted here
  service_times:
    standby:
      execution_model: processor_sharing
      replica_queue_model: independent_random
      concurrency_limit_per_replica: 16
      cpu_bandwidth:
        quota_seconds: 0.1
        period_seconds: 0.1
        parallelism: 16
      distribution: deterministic
      mean: 0.03
```

The three bandwidth fields are explicit:

- `quota_seconds`: CPU-seconds granted per replica per period.
- `period_seconds`: time between budget replenishments.
- `parallelism`: the declared maximum physical CPU parallelism available to
  runnable work within a replica. One visit uses at most one physical CPU.

Quota and period must be positive, at least 1 ms, with a period no greater
than one second. The quota/period core rate cannot exceed parallelism. The
native parser independently validates the same constraints. The extension
requires independent replica queues so each replica has its own budget;
pooled replica budgets are rejected.

**Service demand keeps the existing normalized units:** measured CPU-seconds
per visit divided by the long-run CPU capacity `quota_seconds / period_seconds`.
For example, 30 ms of CPU work with a 50 ms quota per 100 ms period is a model
mean of 60 ms. At a one-core average limit, normalized demand equals physical
CPU time. Blocking delays remain wall-clock seconds and are not divided by
CPU capacity. Stage-specific CPU demand uses the same normalized convention.

## Execution and analytical behavior

Each replica starts with a full budget and replenishes it at multiples of its
declared period. Idle periods do not accumulate credits. Active visits make
parallel progress up to the declared bound and share physical CPUs when that
bound is reached. Exhausting the budget stops CPU progress until the next
period. Blocking does not consume CPU budget; it retains the existing
concurrency-admission semantics. Resource work remains scheduled after a
client deadline. Replicas have separate budgets, while classes and workflow
stages using one resource share that resource's budget.

All replicas currently use the same initial period phase. Warmup reduces
initial-state effects, but synchronized phase can matter for correlated
cross-replica workloads and is another assumption to validate. There is no
extra burst-credit allowance.

The simulator's busy-replica and processor-sharing backlog statistics describe
replicas with outstanding CPU work, including work waiting for quota refill.
They are not instantaneous cgroup CPU-usage measurements. Retain measured CPU
counters and use normalized offered CPU work for the long-run load check.

Long-run offered work and minimum stable replica equations retain their
existing normalized capacity units. They are necessary load checks, not a
steady-state stability certificate. Strict analytical latency evaluation
abstains for periodic quota; ordinary M/G/1-PS results are not used as if
they described the new scheduler. The calibration identity includes the
bandwidth parameters and the changed native semantics version.

## Obtaining the parameters

Quota and period can be collected from the container runtime's `cpu.max`
(cgroup v2), the CFS quota/period files (v1), or exposed cAdvisor
`container_spec_cpu_quota` and `container_spec_cpu_period` metrics. cAdvisor
values are in microseconds. Match the application container and retain the
common characterization window; collector labels depend on the installation.
CPU requests alone do not determine a hard quota or physical CPU affinity.

Affinity/cpuset and the application's runnable worker bound inform parallelism.
A node CPU count provides only an upper bound: other workloads can reduce
available capacity. Do not fit parallelism to target latency and call the
result an independent validation. If availability is not identified, retain
the uncertainty, report sensitivity, or abstain from a reliable prediction.

The `pgp1` retrospective replay uses the observed quota/period and a declared
16-way upper bound from its worker CPU counts. Historical per-process affinity
was not retained. Sensitivity at 1, 2, 4 and 8 is reported alongside that
declaration. The original model, scores, target eligibility, residuals and
CPU means are preserved. This replay can diagnose a modeling mismatch;
fresh targets are needed to validate improved accuracy and safety.

Replay the retained evidence locally with:

```bash
python scripts/replay_postgres_cpu_bandwidth.py \
  results/postgres-pilot-pgp1-20261116/data \
  --cpu-profile results/postgres-read-audit-pgp1-20261117/cpu-profile.json \
  --output results/postgres-read-audit-pgp1-20261117/replay
```

The output path must be new. The script verifies original evidence receipts,
records model/source hashes, runs the simulation ablations, and scores only
after writing predictions. The hypothesis itself was chosen after the old
targets were inspected, so this ordering does not turn the replay into a
prospective experiment.
