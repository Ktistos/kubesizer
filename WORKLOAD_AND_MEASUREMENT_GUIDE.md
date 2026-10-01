# Analytical input guide

Provide one workload YAML, or a workload plus a portable measurement manifest
and the CSVs that manifest references. `workload.minimum.yaml` is the smallest
direct example; `examples/portable_evidence/` illustrates measured inputs.

Direct workloads contain `request_classes` and `traffic_model`. Each request
class declares a traffic share, a mean SLO (`end_to_end_latency_slos.mean_seconds`),
and routing, visit ratios, an ordered workflow or a synchronous call graph.
`traffic_model.entry_services` supplies external arrival distributions and rates;
`traffic_model.service_times` supplies per-service execution distributions.

Supported distribution inputs include exponential, deterministic, gamma,
lognormal, Weibull, empirical durations, empirical interarrivals and arrival
timestamps. Analytical calculation uses their moments. Means are seconds and
rates are requests/second. SCV means variance divided by the squared mean.
An arrival trace does not make the analytical solver preserve burst ordering:
its queueing approximation uses arrival moments.

`processing_slots_per_replica` declares CPU execution capacity;
`concurrency_limit_per_replica` declares an admitted request limit. They are
distinct. `non_cpu_blocking_time` releases CPU but may occupy admitted
concurrency. The baseline records these fields and refuses to certify their
unsupported interactions. It never silently drops them to produce a plan.

`latency_components` declares named boundary contributions such as
`client_to_entry`. The sum of their means is included once in end-to-end latency.
The observation boundary must agree with the SLO and supplied measurements.

With `--measurements`, declare service CPU capacity and topology in the portable
workload; rates and CPU demand come from aligned measurement windows. Stable
external-minus-entry mean latency can provide a measured boundary residual.
Missing or ambiguous evidence produces an error or explicit sensitivity variants.
The CLI requires resolved call semantics before recommending replicas.

Run `./scripts/kubesizer --help` for current options. Results contain recommended
replicas only. `--out path.json` optionally persists the JSON envelope.

The [historical detailed schema](docs/history/WORKLOAD_AND_MEASUREMENT_GUIDE.md)
also describes simulation fields belonging to the simulation branch.
