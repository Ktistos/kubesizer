# PostgreSQL standby sizing at 36.87 requests/s

A subsequent trial compared aggregate and process-level CPU inputs using fresh held-out rates. [Portable-telemetry sizing follow-up](../postgres-portable-sizing-pgs2-20261119/REPORT.md)

Adding a second read-serving standby reduced mean read latency from **2,031 ms to 240 ms** in the isolated PostgreSQL trial. Neither layout established feasibility under the predeclared 250 ms mean-latency SLO. Both Kubesizer evaluators abstained in the forecasts frozen before the target runs. [Physical scores](data/scores.json) · [Frozen forecasts](data/predictions-frozen.json)

## Design

On 19 November 2026, the trial compared one primary plus one read standby with the same primary plus two read standbys. Both layouts used local durable write acknowledgement, eventual reads, the same dataset, and the same 75% read and 25% write workload. The two-standby layout divided reads by key parity. Each of the three matched pairs reused the scheduled arrivals, request classes, and keys. A separate gateway and localhost port forward isolated the pilot from the original deployment. [Protocol](../../docs/POSTGRES_REPLICA_SIZING_PILOT.md) · [Matched schedules and run audit](audit.json)

Kubesizer characterized the one-standby layout in two independent 12 RPS runs. The two-standby model added a replay stream and used the first standby's per-request demands and replay proxy for the second standby. The target rate was 90% of the CPU-derived reference for the one-standby layout. This was a workload design point. The pilot used PostgreSQL process CPU and replay measurements in addition to request telemetry. [Characterization](data/characterization.json) · [Protocol and forecast bindings](data/prediction-receipt.json)

## Physical results

The estimates below are means of three physical run means per layout. Intervals are two-sided 95% Student t intervals over those run means. The predeclared gate requires the upper bound to be at or below 250 ms for both classes, together with the client and replication health checks. [Per-run results](data/target-results.json) · [Scoring procedure](../../scripts/run_postgres_sizing.py)

| Layout | Class | Mean | 95% interval | Individual run means | SLO classification |
| --- | --- | ---: | ---: | --- | --- |
| One standby | Read | 2,031 ms | 177–3,886 ms | 2,591, 1,184, 2,319 ms | Inconclusive by interval |
| One standby | Write | 237 ms | 0–527 ms | 372, 170, 170 ms | Inconclusive by interval |
| Two standbys | Read | 240 ms | 97–383 ms | 307, 203, 211 ms | Inconclusive by interval |
| Two standbys | Write | 169 ms | 166–172 ms | 170, 170, 168 ms | Feasible in the pilot |

Every one-standby read run exceeded 250 ms. The intervals were wide with three repetitions, so its preregistered classification remained inconclusive. The two-standby layout had an 88% lower read mean, but its upper interval still exceeded the SLO. No lowest feasible replica cost could be verified among the two layouts. [Physical scores](data/scores.json)

The six targets attempted 20,066 tagged requests. There were 21 failed reads, all in the final two-standby repetition. PostgreSQL reported conflicts with recovery. These were 0.83% of reads in that repetition, below the predeclared 1% threshold. All runs passed the client scheduling and evidence checks, drained, and stayed below the sampled replay backlog limit. These checks describe the measured runs. The duration and replication count limit conclusions about sustained stability. [Client traces and replication snapshots](data/runs) · [Run audit](audit.json)

PostgreSQL processes on the single standby consumed 0.974–0.994 of its one allotted CPU core over the audited measurement intervals. Each standby in the two-standby layout consumed 0.452–0.488 cores. The gateway-to-SQL timer includes connection-pool waiting and SQL execution. It recorded a mean of about 1,867 ms for reads with one standby and 77 ms with two, accounting for most of the client latency difference. [Process CPU and boundary diagnostics](audit.json)

## Forecasts and calibration correction

The original analytical workflow abstained because the declared finite admission, blocking, and call behavior were outside its supported assumptions. The simulation workflow rejected the one-standby candidate during warm-up calibration and did not find a reliably feasible configuration in the two-standby candidate. Neither workflow issued an accepted plan for this target. [Frozen CLI responses](data/predictions-frozen.json)

The one-standby rejection exposed a warm-up calibration error. The standby received about 27.66 read visits/s at 32.49 ms of CPU per visit and 9.22 replay visits/s at 0.17 ms per visit. The old check applied the read cost to the combined rate, so it reported negative slack. The corrected calculation weights the service demand by the actual mix of visits. It reports 0.90 modeled processing utilization and about 4.10 visits/s of effective capacity slack for that standby. The correction applies to mixed workloads generally. [Calibration change](../../calibration/warmup_parameters.py) · [Regression test](../../tests/test_multiclass_service_demands.py)

A separate replay used the corrected code and identical frozen inputs. Both candidates reached the feasibility search, and neither was accepted. A retrospective finite-window diagnostic from the frozen parameters estimated **464 ms** read mean for one standby and **225 ms** for two. The one-standby prediction substantially underpredicted the measured mean of 2,031 ms. The two-standby predicted write mean was **262 ms**, above the observed 169 ms. These estimates are diagnostic finite-window means and were excluded from the prospective decision. [Corrected CLI replay](after-warmup-fix.json) · [Finite-window diagnostics](modeled-finite-windows.json)

## Implication for arbitrary deployment sizing

The pilot demonstrates a measurable change in read latency across physical PostgreSQL layouts under a shared workload. The one-layout characterization did not predict the size of the near-capacity slowdown, and the tested scale-out did not establish SLO feasibility. Recovery conflicts also affected read success even though the replicated data path drained after each trial.

To test whether the model can distinguish which layout is sufficient, another prospective sweep can compare absolute rates nearer the read SLO boundary with more physical repeats. The client-to-gateway delay and SQL-path delay should remain separately observable. An independent characterization point can test whether the observed difference in standby response survives the transfer assumption. The role-aware PostgreSQL instrumentation leaves open which parameters can be recovered from portable telemetry alone. [Cross-deployment benchmark](../cross-deployment-sizing-20261119/REPORT.md)

## Evidence and lifecycle

The first launch stopped at Kubernetes manifest validation before any data collection. The retry used separate manifests for the built-in resource types and retained server validation. It completed all six target runs, and cleanup confirmed zero pilot pods. The simulator comparison campaign remained paused. All physical traces, frozen inputs and the executed source are archived with the trial. [First launch](launch-attempt/status.json) · [Cleanup receipt](data/cleanup.json) · [Executed source archive](execution-source.tar.gz)
