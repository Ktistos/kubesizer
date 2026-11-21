# PostgreSQL sizing with aggregate CPU measurements

The aggregate-counter and process-counter inputs produced the **same three frozen sizing decisions**. The 18 held-out runs confirmed the least-cost measured feasible layout at **20 and 28 requests/s**. At **34 requests/s**, the selected two-standby layout remained inconclusive because its read-latency interval crossed the SLO. [Physical scores](data/scores.json) · [Frozen forecasts](data/predictions-frozen.json)

## Characterization

The experiment compared one and two read-serving PostgreSQL standbys behind the same gateway and primary. Both layouts used local durable commits and the same 75% read and 25% write workload in the targets. The mean client-latency SLO was 250 ms for each class. Both methods obtained their inputs from the one-standby layout, and both applied the same demand-transfer assumption when modeling the second standby. [Experiment protocol](../../docs/POSTGRES_PORTABLE_SIZING_PROTOCOL.md) · [Protocol before launch](protocol-before-launch.json)

The aggregate method used cumulative database-container CPU, an idle baseline and isolated read and write runs. The process method used CPU counters from the PostgreSQL query and replay processes in those same runs. Both methods counted completions from the same trace within each counter's measurement interval. Both shared the measurements of gateway-to-SQL latency and client-to-gateway residual from two additional mixed runs. [Aggregate profile](data/profile-aggregate.json) · [Process profile](data/profile-process.json)

| Incremental CPU demand | Aggregate counter | Process counters |
| --- | ---: | ---: |
| Standby read | 31.72 ms per read | 31.67 ms per read |
| Primary write | 1.65 ms per write | 1.20 ms per write |
| Standby replay | 0.70 ms per write | 0.13 ms per write |

The two estimates of read demand differed by about 0.05 ms. The aggregate estimate attributed more CPU to writes and replay. It includes workload-dependent container CPU outside the selected query and replay processes. For the tested mix, the difference between replay estimates corresponds to about 0.005 CPU cores per standby at 34 requests/s, small relative to the read workload. [Counter windows and aligned request counts](data/profile-aggregate.json)

The Prometheus API probe from the lab timed out, and kubelet counters were unavailable through the experiment identity. The aggregate arm used the equivalent type of cumulative container CPU measurement from the container cgroup. The trial evaluates the sufficiency of aggregate data under the controlled measurement design. It still required class-isolated traffic, downstream timing and declared replication and routing behavior. [Telemetry probe](telemetry-probe.log) · [Runner](../../scripts/run_postgres_portable_sizing.py)

## Held-out layout decisions

Each rate had three matched physical pairs, using the same request schedule and key seeds across layouts. The forecasts were frozen before any target run. The targets logged **32,072 requests and zero failures**. All nine pairs had matching request ordinals, classes and scheduled offsets. [Per-run results](data/target-results.json) · [Verification](verification.json)

| Offered requests/s | Frozen choice from both inputs | One standby | Two standbys | Decision outcome |
| ---: | --- | --- | --- | --- |
| 20 | One standby | Feasible | Feasible | Cheapest measured feasible layout selected |
| 28 | Two standbys | Inconclusive | Feasible | Selected layout passed. The cheaper layout remained inconclusive. |
| 34 | Two standbys | Inconclusive | Inconclusive | Observed SLO feasibility unresolved |

The following are means of three physical run means. The intervals are the predeclared 95% Student t intervals across runs. Both class upper bounds must be at or below 250 ms, alongside the failure, scheduling, drain and replication checks. [Detailed scores](data/scores.json)

| Offered requests/s | Layout | Read mean and interval | Write mean and interval |
| ---: | --- | ---: | ---: |
| 20 | One standby | 201 ms, 195–207 ms | 177 ms, 144–211 ms |
| 20 | Two standbys | 198 ms, 197–199 ms | 169 ms, 169–170 ms |
| 28 | One standby | 219 ms, 216–221 ms | 203 ms, 57–349 ms |
| 28 | Two standbys | 199 ms, 197–200 ms | 169 ms, 168–170 ms |
| 34 | One standby | 429 ms, 0–935 ms | 191 ms, 99–283 ms |
| 34 | Two standbys | 226 ms, 117–334 ms | 169.0 ms, 168.6–169.4 ms |

At 28 requests/s, the one-standby read interval passed. A slower write run widened the write interval across the three repetitions. The two-standby configuration was the only layout with both classes verified at that rate. This leaves the true minimum at 28 open until the cheaper layout's write behavior can be resolved.

At 34 requests/s, the one-standby read means were approximately 402, 241 and 645 ms. The two-standby read means were 276, 200 and 201 ms. The single standby used 0.81–0.90 CPU cores over the sampled windows. With two standbys, each used 0.40–0.49 cores. The two-standby mean fell below the SLO, yet one run exceeded it and the upper interval remained above it. This was an accepted recommendation with inconclusive physical evidence. [Class-level results](data/scores.json) · [Observed container CPU](verification.json)

## What this shows

The aggregate measurements supported the same sizing decisions as process-level measurements for this read-heavy, local-commit PostgreSQL workload. At two of the three rates, the chosen plan met the measured feasibility checks. The comparison isolates the input measurement difference by holding the simulation, request timings, mechanism declarations and physical targets constant. A more write-intensive mix, a different replication acknowledgement path, or workload classes that an operator cannot isolate may need additional evidence.

The runs used one cluster and a shared VM-to-gateway client path. Three physical runs per layout and rate leave appreciable uncertainty around the SLO boundary. Cross-deployment evidence is still needed for the arbitrary-deployment thesis goal. [Earlier PostgreSQL trial](../postgres-sizing-pgs1-20261119/REPORT.md) · [Cross-deployment benchmark](../cross-deployment-sizing-20261119/REPORT.md)

## Evidence and cleanup

The pilot completed all 18 targets on 19 November 2026. The execution source archive was checked against its receipt. The archived data reproduced the original scores, and all three original database pod templates were restored. The pilot pods are down and the comparison campaign remained paused. [Verification](verification.json) · [Executed source](execution-source.tar.gz) · [Restoration receipt](data/member-template-restoration.json)
