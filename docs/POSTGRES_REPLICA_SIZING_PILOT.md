# PostgreSQL read-standby sizing pilot

This pilot compares two physical PostgreSQL replica layouts at the same offered load. The question is whether a sizing prediction made from the one-standby layout selects the cheapest measured layout that satisfies the latency and health criteria. It reuses the isolated PostgreSQL dataset and containers from [pgp1](POSTGRES_PRELIMINARY_PILOT.md).

| Candidate | Active database members | Read routing | Write acknowledgement |
| --- | ---: | --- | --- |
| One read standby | 2 | All reads to the first standby | Local durable commit on the primary |
| Two read standbys | 3 | Equal routing by key parity | Local durable commit on the same primary |

The request mix is 75% reads and 25% writes. The read and write SQL, data, query keys, member resource limits, and client-to-gateway measurement boundary are shared across arms. Each pair reuses the same offered-arrival and operation seeds. Pair order alternates after a seeded first order. Reads use eventual consistency and have no read-your-writes guarantee. A retained replication backlog bound is a health check for this pilot.

## Characterization and freeze

Two low-load runs characterize the one-standby candidate at 12 RPS. The profile uses SQL backend and replay process CPU counters, low-load query times, client-to-gateway residuals, and a replay-delay witness. The latter includes the flush marker query, polling and transport. These measurements are explicit PostgreSQL pilot instrumentation and go beyond endpoint telemetry. The second-standby model transfers the first standby's per-request demands and witness samples, adds the second replication stream, and changes the read routing to 50/50. The physical test checks this transfer assumption. Any unmodeled change in primary WAL sender work or shared storage contention can affect the result.

The target rate aims for 90% of the CPU-derived one-standby reference and is capped at 42 RPS. That fraction is a design label. The CLI evaluates both declared candidates using the analytical and simulation modes and records any abstentions. The protocol, inputs, candidate models, CLI responses and target order are hashed before the first target. The candidate cost counts active database members. The common gateway is excluded from both costs.

## Matched targets and scoring

Each layout receives three runs at the frozen target rate. Each target has 30 seconds of warm-up and a 90-second arrival window. Client overload and missing replication evidence produce an inconclusive classification. A repeat with a class failure rate above 1%, or a lower interval for mean latency above the 250 ms class SLO, makes a layout infeasible under the pilot criteria. Feasibility requires upper intervals at or below the SLO for both classes, drained arrivals, and no sampled replay backlog above 1 MiB in any repeat. Intervals are calculated over the three physical run means. With three runs at one load, these are exploratory results. Cohort drain and sampled backlog describe behavior during the pilot. Steady-state guarantees require longer and broader evidence.

## Isolation

The runner verifies that the original pilot and the comparison campaign are idle. It uses a separate gateway deployment and localhost port forwarding. The existing PostgreSQL members use the retained lab volumes and the original node placement. The runner uses local-commit SQL sessions without changing the primary's persistent acknowledgement setting. Cleanup scales the pilot members and new gateway to zero, verifies pod termination, and records a receipt. It leaves the original and newly collected evidence available for inspection.

[Runner](../scripts/run_postgres_sizing.py) · [Previous fixed-layout pilot audit](../results/postgres-pilot-pgp1-20261116/AUDITED_SUMMARY.md)
