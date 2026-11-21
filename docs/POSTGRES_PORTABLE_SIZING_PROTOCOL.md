# PostgreSQL sizing with aggregate and process CPU measurements

This trial compares two characterization inputs on the same physical PostgreSQL sizing problem. It extends the earlier [one-versus-two standby pilot](../results/postgres-sizing-pgs1-20261119/REPORT.md), which demonstrated a latency difference at a single rate while leaving SLO feasibility unresolved.

## Candidates and targets

Both layouts use the same primary, dataset, local durable commit setting, and eventual reads. One layout sends all reads to a single standby. The other uses two standbys and routes reads by key parity. Active database members are the cost units. The common gateway and VM-to-gateway client boundary are shared.

The absolute offered targets are 20, 28 and 34 requests/s. These bracket the approach to the earlier one-standby slowdown without repeating the previous 36.87 RPS point. They use the same 75% read and 25% write workload and the 250 ms client mean SLO per class. Three physical pairs are planned at each rate. The layout order alternates after seeded randomization, and each matched pair uses the same arrival, class, and key seeds in both layouts. Each run has 25 seconds of warm-up and a 65-second arrival window. The target protocol and models are saved before target collection.

## What differs between the two inputs

| Input | Aggregate-counter arm | Process-counter arm |
| --- | --- | --- |
| Read CPU demand | Standby container CPU above the idle rate during read-only traffic, divided by completed reads | CPU of the read backend processes during the same read-only run, divided by completed reads in its CPU interval |
| Local write CPU demand | Primary container CPU above the idle rate during write-only traffic, divided by completed writes | CPU of the write backend processes during the same write-only run, divided by completed writes in its CPU interval |
| Replay CPU per write | Standby container CPU above the idle rate during write-only traffic, divided by completed writes | Standby startup process CPU during the same write-only run, divided by completed writes in its CPU interval |

In each arm, request counts are aligned to the retained counter timestamps using gateway completion events. The two arms share request latencies and the client-to-gateway residual from the same low-load mixed runs. For both arms, the second layout transfers the measured standby demand to its additional standby. CPU demand for WAL replay remains in the model as background work. Both arms omit the separate replay-delay proxy from the earlier pilot, since acknowledgement in this trial waits for local commit. Sampled replay backlog still contributes to physical eligibility.

An unsupported or nonpositive demand estimate is retained as an abstention. An aggregate CPU estimate may include workload-dependent background work and fails to identify a class contribution unless the class can be isolated or a comparable design separates the effects. The process arm has additional attribution to the query and replay processes. Neither arm infers database routing from CPU alone.

## Collection scope

The lab monitoring endpoint could be discovered, but the in-cluster Prometheus API probe timed out, and the VM's pilot identity could not read the kubelet counters. The trial collects the database container's cumulative CPU from the container's cgroup instead. It exposes only the standard aggregate counter through the existing lab observer and retains the data collection provenance. This tests a portable *measurement type* when an operator can obtain it, along with class and downstream timing measurements. It does not validate a Prometheus or Istio-only capture workflow.

The cgroup sampler and the process observer are present for both layouts and both input arms. After the pilot, the runner scales down the isolated members and gateway and restores the original database pod templates. The existing comparison campaign stays paused.

## Decision scoring

Results report the selected layout, empirical SLO eligibility, cheaper measured feasible alternatives, and abstentions. A feasible classification uses the earlier pilot's confidence-interval, failure, load-generator and replication checks. Three physical runs at a rate remain exploratory. Physical outcomes and independent windows stay separate from characterization.
