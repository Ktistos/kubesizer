# Kubesizer usage and input mapping

Kubesizer recommends a replica count for each modeled service. The current
tool deliberately does not optimize node placement, parse a node-resource
inventory, or output pod-to-node assignments.

## Command

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation
```

`--measurements` is optional when `workload.yaml` already contains a complete,
manually characterized traffic model. `--out` is optional; without it the tool
does not create a result file.

The public evaluation choices are:

| Option | Meaning |
|---|---|
| `--replica-evaluation analytical` | Fast analytical stability and supported mean-latency evaluation |
| `--replica-evaluation simulation` | Fitted-variability characterization followed by calibrated independent simulation for supported mean or p95 SLOs |

Replica optimization always uses exact uniform-cost search. There is no search
mode, placement mode, resource-file argument, or hybrid mode.

The removal of the weaker simulation variants does not remove the analytical
queueing path. Analytical evaluation remains available for compatible mean-SLO
workloads, and analytical stability is still used to screen simulation
candidates. The simulation path has one characterization method: fitted
variability.

## Files supplied by an operator

### `workload.yaml`

The workload declares the sizing question and the static facts needed to
interpret measurements:

- service names and namespaces;
- per-replica CPU request/limit context;
- execution-model declarations and per-replica concurrency limits for services
  whose positive blocking residual will be modeled;
- request classes, entry services, call graph, and call execution semantics;
- latency boundary and explicitly measured latency components;
- mean or percentile end-to-end SLOs;
- forecast multipliers and maximum capacity utilization;
- an optional same-node/cross-node communication-delay mixture.

CPU and memory declarations identify the conditions under which evidence was
collected. They are not used to assign pods to nodes or certify Kubernetes
schedulability.

### `measurements.yaml` and CSV evidence

The manifest points to portable observation tables. Depending on available
evidence, these can include:

| Evidence | Used to derive |
|---|---|
| Service intervals | Per-service throughput, CPU demand, replica count, utilization, and health gates |
| Service-class intervals | Request-class-specific demand and flow attribution |
| External intervals | External class rates and observed end-to-end means |
| Service edges | Calls per source request and the directed service graph |
| Service latency histograms | Per-service response mean/p50 used to infer blocking residuals and fit leaf-service variability |
| External latency histograms | Observed end-to-end latency statistics |
| External arrival samples | Burst-preserving arrival traces |
| Capacity observations | Confidence-bounded empirical replica floor and forecast envelope |

The portable path checks common, non-overlapping UTC windows, counter
semantics, replica stability, and evidence health before characterization.

### Direct model path

For synthetic studies or externally characterized systems, a self-contained
`workload.yaml` may directly declare `traffic_model`, including external
arrival distributions, service-time distributions, request-class-specific
demands, blocking times, input uncertainty, and communication latency.

## Mapping into the simulation

Characterization freezes operator evidence into one simulation workload:

| Frozen value | Main source | Simulator use |
|---|---|---|
| External interarrival process | External counts and optional timestamp traces | Generates external requests |
| CPU service-time mean | CPU-seconds divided by completed requests | Processing work per visit |
| CPU service time | CPU-seconds/request divided by declared CPU capacity | Deterministic processor-sharing work per visit |
| Non-CPU blocking mean | Low-load service-response mean minus CPU, downstream, and communication means | Holds declared concurrency while releasing CPU |
| Leaf blocking variability | Blocking mean plus the compatible service-response p50 residual | Fits one capped lognormal SCV; falls back to the same mean deterministically when right-skew cannot be identified |
| Call graph and call counts | Service-edge counters or declared topology | Creates downstream visits |
| Processing/concurrency capacity | Explicit portable service declarations | Controls queue admission and execution |
| Latency components | Aligned boundary measurements or explicit distributions | Adds ingress/client/residual latency once |
| Communication mixture | Same/cross-node probes and `cross_node_probability` | Samples service-edge transit delay |
| Input uncertainty | Aligned observation epochs | Resamples correlated rate/demand changes |

The frozen workload is an internal model, not an additional file the operator
must author.

## Workflow

```text
load workload and optional portable evidence
                    |
                    v
characterize and freeze either the analytical CPU model
or the fitted-variability simulation model
                    |
                    v
calibrate warm-up and measurement durations (simulation mode)
                    |
                    v
search bounded replica vectors
                    |
                    v
validate the selected vector with fresh seeds (simulation mode)
                    |
                    v
emit per-service recommended_replicas
```

The output intentionally stops at replica counts. Apply those counts through
the deployment controller and let Kubernetes plus the operator's affinities,
taints, topology-spread constraints, and resource policies make scheduling
decisions.

## Scope of the recommendation

An exact search result is exact only within the declared model and replica
bounds. If a cheaper simulation candidate remains statistically inconclusive,
the returned feasible result is explicitly marked as not proven minimal. A
simulation confidence interval quantifies simulation uncertainty; it does not
cover missing bottlenecks or a changed deployment.
