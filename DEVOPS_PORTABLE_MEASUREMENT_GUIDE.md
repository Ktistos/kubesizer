# DevOps guide: collecting portable Kubesizer measurements

This guide explains how to turn telemetry from an arbitrary Kubernetes
deployment into the portable evidence package accepted by Kubesizer.

The engineer supplies two logical inputs:

```text
workload.yaml       declarations: services, topology, SLO and forecast
measurements.yaml   manifest pointing to normalized measurement CSV files
```

Kubesizer then characterizes the deployment, constructs its internal model,
sizes replicas, and—when simulation is selected—validates the recommendation
with fresh simulation seeds. The engineer does not write the internal frozen
simulation model.

The complete example package is in
[`examples/portable_evidence`](examples/portable_evidence/).

## 1. Understand what “portable” means

Portable means that the evidence can be collected from ordinary operational
interfaces without synthetic ground truth or application-specific changes to
Kubesizer. It does not mean that every cluster exposes identical Prometheus
labels.

The collection adapter may differ between clusters, but it must normalize data
to these identities:

```text
logical service
Kubernetes namespace
pod
request class
common UTC observation window
```

The main sources are:

- Kubernetes workload declarations;
- Istio completed-request counters and latency histograms;
- Prometheus container CPU, throttling and memory metrics;
- ingress, client or load-generator telemetry for external requests;
- controlled load tests for empirical capacity.

Some facts remain declarations because passive telemetry cannot identify them
reliably:

- the latency SLO and its boundary;
- whether sibling downstream calls are sequential or parallel;
- configured worker, thread, semaphore or connection-pool limits;
- the future load forecast.

Observed maximum concurrency is only a lower bound on a configured limit. Do
not copy a passive peak into `concurrency_limit_per_replica` unless a controlled
saturation test and application configuration establish that it is the actual
ceiling.

## 2. Know which files are needed

Only `service_intervals` is mandatory to parse a measurement manifest. That is
not enough for a defensible sizing run. The practical package is:

| File | Practical status | Purpose |
|---|---|---|
| `workload.yaml` | Required | Service declarations, request classes, topology, SLO and forecast |
| `measurements.yaml` | Required | Versioned manifest and CSV paths |
| `service-intervals.csv` | Required | Per-pod requests, CPU and optional health/latency context |
| `external-intervals.csv` | Required for portable sizing | External rate and observed SLO statistic per request class |
| `capacity-observations.csv` | Required by the current CLI stability floor | Independent healthy-capacity evidence for every service |
| `external-latency-histograms.csv` | Required for an observed percentile SLO | End-to-end histogram at the declared boundary |
| `service-latency-histograms.csv` | Recommended for simulation | Service p50 and tail evidence used by blocking characterization |
| `service-class-intervals.csv` | Recommended | Class-specific service visits and flow checks |
| `service-edges.csv` | Required if the call graph is not declared | Directed downstream call counts |
| `external-arrival-samples.csv[.gz]` | Required for a latency recommendation | Measured burst ordering used by trace-block simulation; throughput-only sizing may use the documented fallback |

For the default policy, collect at least three complete observation windows and
at least three independent healthy capacity runs for each tested service and
replica count.

## 3. Prepare the cluster and collection worksheet

You need:

- `kubectl` access to read workload specifications and pod identities;
- a Prometheus-compatible query endpoint;
- Istio standard metrics, or equivalent request counters and histograms;
- container metrics from kubelet/cAdvisor;
- `kube-state-metrics` or Kubernetes API access for resource declarations;
- a stable request-class label such as a bounded route or operation name;
- a load generator for capacity runs.

Before querying anything, create a worksheet like this:

| Kubesizer service | Namespace | Kubernetes workload | Application containers | Entry service? |
|---|---|---|---|---|
| `api` | `production` | `deployment/api` | `api` | yes |
| `catalog` | `production` | `deployment/catalog` | `catalog` | no |

Use one stable, low-cardinality request-class name for each user operation,
for example `browse` or `checkout`. Never use raw URLs containing IDs, trace
IDs, user IDs or other unbounded values.

The PromQL below is a template. Replace label names with those exposed by your
mesh. In particular, verify `destination_workload`,
`destination_workload_namespace`, `destination_pod`, `source_workload` and the
request-class label in your Prometheus data before automating export.

## 4. Write the declaration skeleton

Start from
[`examples/portable_evidence/workload.yaml`](examples/portable_evidence/workload.yaml).
A minimal structure is:

```yaml
services:
  api:
    namespace: production
    cpu_request_cores: 0.5
    concurrency_limit_per_replica: 32

  catalog:
    namespace: production
    cpu_request_cores: 0.25

request_classes:
  browse:
    concurrency_weight: 1.0
    entry_services:
      api: 1.0
    calls:
      api:
        catalog: 1.0
    call_execution: sequential
    latency_boundary: ingress
    end_to_end_latency_slos:
      percentile:
        quantile: 0.95
        maximum_seconds: 0.250

load_forecast:
  multipliers: [1.0, 1.25]

sizing_policy:
  maximum_capacity_utilization: 0.85
```

The `calls` graph is directed: `api -> catalog` means that `api` initiates a
catalog call. Do not add a reverse edge for the response.

The current schema declares one `call_execution` mode for a request class. If
one class mixes sequential and parallel sibling-call behavior in a way that
materially affects latency, split it into better-defined request classes or
record the mismatch as a model limitation.

## 5. Extract Kubernetes service facts

Inspect every application container in each modeled workload:

```bash
kubectl -n production get deployment api \
  -o jsonpath='{range .spec.template.spec.containers[*]}{.name}{"\t"}{.resources.requests.cpu}{"\t"}{.resources.limits.cpu}{"\t"}{.resources.requests.memory}{"\t"}{.resources.limits.memory}{"\n"}{end}'
```

For `workload.yaml`:

- convert CPU quantities to cores: `500m` becomes `0.5`;
- sum CPU requests across application containers in one replica;
- exclude the `istio-proxy` request because service CPU measurements also
  exclude the proxy;
- record CPU limits only when they are consistently defined;
- convert memory quantities to bytes when retaining memory context;
- take concurrency limits from application/configuration knowledge, not from a
  passive traffic peak.

Record the image/version, configuration revision and node pool outside the
schema as experiment metadata. Evidence should be recollected when these
conditions materially change.

## 6. Choose valid observation windows

Use common, fixed-duration, non-overlapping UTC windows. Three five-minute
windows are a reasonable minimum example, not a universal duration:

```text
2026-09-16T10:00:00Z -> 2026-09-16T10:05:00Z
2026-09-16T10:05:00Z -> 2026-09-16T10:10:00Z
2026-09-16T10:10:00Z -> 2026-09-16T10:15:00Z
```

During all retained windows require:

- fixed application version and configuration;
- fixed replica count for every modeled service;
- no rolling update, pod restart, OOM kill or autoscaler action;
- completed warm-up before the first window;
- stable request-class definitions and routing;
- no missing Prometheus scrapes;
- acceptably low 5xx errors and CPU throttling;
- enough requests to estimate the declared latency statistic.

Every modeled service must appear on the same window grid. Other interval CSVs
may omit irrelevant subjects, but any window they contain must be one of the
`service-intervals.csv` windows.

Evaluate counter queries at the exact end of each window. For example:

```bash
PROMETHEUS_URL=http://prometheus.monitoring.svc:9090
WINDOW_END=2026-09-16T10:05:00Z

curl -Gs "$PROMETHEUS_URL/api/v1/query" \
  --data-urlencode "time=$WINDOW_END" \
  --data-urlencode 'query=sum(increase(istio_requests_total{reporter="destination"}[5m]))'
```

The response is still provider-shaped JSON. Your exporter must map its labels
and values into the normalized CSV rows described below.

## 7. Build `service-intervals.csv`

The required header is:

```csv
start,end,service,namespace,pod,request_count,cpu_seconds
```

The recommended full header is:

```csv
start,end,service,namespace,pod,request_count,cpu_seconds,errors_4xx,errors_5xx,throttled_seconds,response_duration_sum_seconds
```

Create one row per `(window, logical service, namespace, pod)`.

### 7.1 Completed destination requests

Use the destination-reporter counter and preserve destination pod attribution:

```promql
sum by (
  destination_workload_namespace,
  destination_workload,
  destination_pod
)(
  increase(
    istio_requests_total{
      reporter="destination",
      destination_workload_namespace=~"production|data"
    }[5m]
  )
)
```

Map the result to `namespace`, `service`, `pod` and `request_count`.
`request_count` means completed destination requests, including successful and
failed responses.

If destination pod attribution is unavailable, add an application per-pod
completion counter or correct mesh telemetry. Do not divide a service-level
count evenly across pods: that biases CPU demand whenever load balancing is
uneven.

### 7.2 Application CPU seconds

For the same pod and window:

```promql
sum by (namespace, pod) (
  increase(
    container_cpu_usage_seconds_total{
      namespace=~"production|data",
      container!="",
      container!="POD",
      container!="istio-proxy",
      image!=""
    }[5m]
  )
)
```

Store the result directly as `cpu_seconds`. Kubesizer derives service CPU
demand from totals:

```text
CPU demand per request
    = sum application CPU-seconds / sum completed service requests

elapsed CPU execution estimate
    = CPU demand per request / declared CPU request cores per replica
```

It does not average per-pod CPU/request ratios, so uneven replica throughput is
handled correctly.

### 7.3 Errors

Use the same destination filters and add protocol-appropriate status filters:

```promql
sum by (destination_workload_namespace, destination_workload, destination_pod) (
  increase(istio_requests_total{reporter="destination",response_code=~"4.."}[5m])
)
```

Repeat with `response_code=~"5.."`. For gRPC, define equivalent failure
semantics using the gRPC response-status label. In every row:

```text
errors_4xx + errors_5xx <= request_count
```

### 7.4 CPU throttling

Use the same application-container filters as CPU:

```promql
sum by (namespace, pod) (
  increase(container_cpu_cfs_throttled_seconds_total{container!="",container!="POD",container!="istio-proxy"}[5m])
)
```

Store this counter increase as `throttled_seconds`.

### 7.5 Service response-duration sum

For the same destination pod cohort:

```promql
sum by (
  destination_workload_namespace,
  destination_workload,
  destination_pod
)(
  increase(
    istio_request_duration_milliseconds_sum{reporter="destination"}[5m]
  )
) / 1000
```

Store seconds in `response_duration_sum_seconds`. This permits a
request-weighted service mean and supports the low-load non-CPU-blocking
decomposition used by simulation.

## 8. Build `service-class-intervals.csv`

Use this header:

```csv
start,end,service,request_class,request_count
```

Export destination completed requests grouped by logical service and a stable
operation label:

```promql
sum by (destination_workload, request_operation) (
  increase(istio_requests_total{reporter="destination"}[5m])
)
```

Istio does not guarantee a useful `request_operation` label automatically.
Populate a bounded operation/route label through mesh telemetry, application
metrics, gateway logs or a collector mapping. Do not use literal high-cardinality
paths.

There must be at most one row per `(window, service, request_class)`, and the
sum of class rows must not exceed the service's pod-attributed request total for
that window.

## 9. Build `external-intervals.csv`

Use this header:

```csv
start,end,request_class,request_count,errors_4xx,errors_5xx,response_duration_sum_seconds
```

Count each top-level external request once. Do not count all internal service
visits. A typical mesh query restricts destination workloads to known entry
services and source workloads to the ingress gateway:

```promql
sum by (request_operation) (
  increase(
    istio_requests_total{
      reporter="destination",
      destination_workload=~"api|checkout-api",
      source_workload="istio-ingressgateway"
    }[5m]
  )
)
```

For each request class:

```text
external rate = request_count / (end - start)
```

The measured class shares should agree with the `concurrency_weight` values in
`workload.yaml`.

Boundary discipline matters:

- with `latency_boundary: ingress`, ingress/entry duration is appropriate;
- with `latency_boundary: external_client`, use client/load-generator latency
  in `external-intervals.csv`. Kubesizer can infer one `client_to_entry` mean
  from that value minus the aligned entry-service mean when the request class
  has a mean SLO and no explicit latency component;
- the automatic fit needs at least three healthy common windows, class-clean
  entry traffic, response-duration sums on both sides, and aligned completion
  counts in at least 80% of candidate windows. It rejects an unstable residual;
- explicitly measured `latency_components` take precedence over automatic
  inference. Use them when DNS, client transit, ingress processing, or other
  boundary pieces have been measured separately;
- never subtract independently aggregated p95 values to create a latency
  component. For a percentile SLO, provide paired samples or an explicit fitted
  component distribution; aggregate means alone make the simulation model
  incomplete.

## 10. Build the latency histogram CSVs

### 10.1 Service histograms

Header:

```csv
start,end,service,upper_bound_seconds,cumulative_count
```

Query and aggregate cumulative buckets across pods before deriving any
quantile:

```promql
sum by (destination_workload, le) (
  increase(
    istio_request_duration_milliseconds_bucket{reporter="destination"}[5m]
  )
)
```

Convert the Istio `le` boundary from milliseconds to seconds. Preserve `+Inf`
as the literal `+Inf`.

### 10.2 External histograms

Header:

```csv
start,end,request_class,upper_bound_seconds,cumulative_count
```

Use the same external cohort, request-class definition and latency boundary as
`external-intervals.csv`.

For both histogram files:

- store cumulative bucket counts, not per-bucket differences;
- do not store only `histogram_quantile(...)` output;
- include exactly one row per `(window, subject, bucket boundary)`;
- include a terminal `+Inf` bucket;
- keep an identical bucket grid for a subject across all windows;
- require non-decreasing cumulative counts;
- make the `+Inf` count agree with the corresponding completed-request count
  within 1% or one request.

For simulation, service response means and p50 values help separate and fit
non-CPU blocking. Retain at least one healthy low-load characterization period;
the current inference gate requires maximum observed CPU utilization at or
below 20%. If a non-leaf service performs parallel downstream calls, aggregate
means cannot identify its local wait reliably. Supply direct blocking evidence
or treat the simulation model as incomplete rather than inventing a residual.

### 10.3 Establish non-CPU blocking evidence

Non-CPU blocking is local time during which a request retains an admitted
application slot but is not consuming CPU, for example database-driver I/O,
lock waiting or an external API wait. It is not downstream service processing
that is already represented by the call graph.

Kubesizer can infer a low-load mean from aligned evidence:

```text
local blocking mean
    = observed service response mean
    - elapsed CPU execution mean
    - modeled downstream response mean
    - modeled communication time
    - independently measured post-release response delay (if supplied)
```

Use only compatible response-bearing rows for the response and CPU terms. Do
not combine a response mean from one traffic cohort with CPU/request from
another. A negative or unidentifiable residual is not evidence for zero
blocking; it indicates incompatible boundaries or insufficient evidence.

For leaf services, a compatible blocking mean and service-response p50 can
identify the spread of the retained capped-lognormal assumption. This is a
model fit, not proof that all waits are lognormal.

When direct application evidence is available, it is preferable. Instrument
wall time, CPU time and named wait spans on the same sampled requests, remove
downstream spans already represented by the graph, and summarize the remaining
local wait as a duration distribution. Declare that evidence under
`service_blocking_times` in `workload.yaml`. Document sampling and boundary
semantics with the package.

If positive blocking is inferred and it should create a finite application
bottleneck, also declare the configured `concurrency_limit_per_replica`.
Kubesizer must not infer that ceiling from the largest passive observation.

#### Optional delay after releasing the service's slot

Response time can include a phase after the application has released its
worker/admission slot. This is different from slot-held blocking. With
independent boundary evidence, declare it explicitly:

```yaml
service_post_release_delays:
  api:
    distribution: deterministic
    mean: 0.003  # Illustrative seconds, not a default or a measured result.
```

This adds response time without holding this service's CPU or admission slot.
A synchronous caller still waits and retains its own slot. The current field
is shared by all request classes visiting the service; it does not describe
class-specific phases, streaming, or arbitrary release/reacquisition patterns.

Measure release and response-boundary timestamps on the same sampled requests,
with instrumentation whose boundaries match the modeled slot. Ordinary Istio
response histograms and CPU counters alone cannot identify this split. A worker
duration shorter than a response duration is a diagnostic clue, not sufficient
evidence that the whole difference occurs after release. Do not derive this
field from the high-load prediction error or holdout results.

Keep it disjoint from downstream response, communication, and client-to-entry
components. A known constant is subtracted before fitting local blocking; a
variable distribution requires independently supplied `service_blocking_times`
because response mean/p50 cannot separate both random components. Deterministic
mean-only evidence still makes a constant-delay assumption, not a measured tail.
Omit the field if absent or unknown; no new CSV is mandatory. Analytical sizing
currently rejects this optional feature rather than silently ignoring it.

### 10.4 Measure optional communication latency

If service-to-service transit is material, run HTTP probes between application
pods with the normal sidecar path:

1. Collect round-trip samples between pods on the same node.
2. Collect equivalent samples between pods on different nodes.
3. Divide round trips by two to obtain one-way distributions.
4. Estimate `cross_node_probability` from actual source/destination placement
   and call traffic.
5. Declare the two distributions and probability under
   `communication_latency` in `workload.yaml`.

Do not hide measured communication time inside local service blocking and then
add it again as communication latency.

## 11. Build `service-edges.csv`

Header:

```csv
start,end,request_class,source_service,destination_service,call_count
```

Use source-reporter completed calls:

```promql
sum by (source_workload, destination_workload, request_operation) (
  increase(istio_requests_total{reporter="source"}[5m])
)
```

Normalize retries consistently with the intended call semantics. The derived
mean call multiplicity is:

```text
edge call rate / source-service visit rate
```

Edge counters reveal direction and multiplicity, but not whether sibling calls
overlap. Declare `call_execution` from code, tracing or design knowledge. If a
complete call graph is already declared in `workload.yaml`, retain edge data as
a flow-balance check.

## 12. Build `external-arrival-samples.csv` (or `.csv.gz`)

Header:

```csv
timestamp,request_class
```

Export the request-start timestamp of each top-level external request from the
load generator, gateway access log or tracing pipeline. Use epoch seconds or
ISO-8601 UTC timestamps. Timestamps must be nondecreasing within each
request-class/entry stream. Equal timestamps are valid and important: they
preserve request batches reported at the gateway clock's resolution.

For a request class with multiple entry services, add `entry_service` and
attribute every row:

```csv
timestamp,request_class,entry_service
2026-09-25T10:00:00.001Z,browse,api
2026-09-25T10:00:00.001Z,browse,catalog-api
```

A multi-entry latency recommendation requires complete attribution for every
declared entry service. A single-entry class may omit the column.

These rows capture ordering and burstiness. They do not replace the
authoritative rate in `external-intervals.csv`; Kubesizer rescales and
block-bootstraps the trace for candidate workloads.
Blocks use a fixed number of consecutive gaps and wrap around the trace end.
Their configured duration is an expected-duration target, not an elapsed-time
cutoff; this prevents the bootstrap from systematically changing offered load.

Use a complete trace. Sampled tracing generally changes interarrival gaps and
must not be presented as complete traffic. Lossless gzip compression is
accepted and is preferable to time-binning because binning destroys within-bin
request ordering.

Each simulated entry stream must contain at least 30 interarrival gaps. This is
a minimum evidence gate, not a claim that 30 observations fully characterize
production bursts; longer traces spanning the expected burst cycles are
strongly preferred.

When a latency SLO is declared, missing or unattributable arrival traces block
the recommendation instead of silently assuming exponential arrivals. The
exponential fallback remains available only for workloads without a latency
SLO, where Kubesizer is not certifying burst-sensitive latency.

## 13. Run independent capacity tests

The current CLI needs a confidence-bounded stability floor for every service.
Passive production windows alone do not establish that floor.

For each relevant replica count:

1. Pin the replica configuration and disable autoscaling.
2. Start a fresh load-test run with a unique `run_id`.
3. Warm up the deployment.
4. Hold a constant offered load for a fixed measurement period.
5. Record offered and completed rates for every service.
6. Check bounded backlog, stationarity, restarts, errors, throttling and any
   predeclared safety latency.
7. Mark `healthy` using those predeclared gates—not whether the result helps the
   desired recommendation.
8. Restore the deployment and repeat as an independent run.

Use this header:

```csv
run_id,service,replicas,offered_rate_rps,completed_rate_rps,healthy
```

Important rules:

- `replicas` is the service's replica count in that run;
- `offered_rate_rps` is the rate offered to that service after accounting for
  request-class visits, not blindly the external client rate;
- `completed_rate_rps` is measured completed service throughput;
- `healthy` is the result of the predeclared backlog, stationarity, restart,
  error, throttling and safety-latency gates; keep the raw gate measurements in
  the load-test record for auditability;
- use at least three independent healthy runs per service and tested replica
  count under the default policy;
- adjacent windows from one continuous run are not independent runs;
- test the expected recommendation range when practical. Extrapolation beyond
  observed replica counts is explicitly conditional.

A run may be healthy even when it does not meet the final product SLO: the
capacity health ceiling and optimizer SLO are separate decisions. Conversely,
a run with growing backlog is not healthy merely because its short measurement
window shows low errors.

## 14. Create `measurements.yaml`

Keep the manifest beside its CSV files and use relative paths:

```yaml
measurement_id: production-observation-20260916
methodology_version: prometheus-istio-kubernetes-v1
service_intervals: service-intervals.csv
service_class_intervals: service-class-intervals.csv
external_intervals: external-intervals.csv
service_edges: service-edges.csv
service_latency_histograms: service-latency-histograms.csv
external_latency_histograms: external-latency-histograms.csv
external_arrival_samples: external-arrival-samples.csv
capacity_observations: capacity-observations.csv
```

Every referenced file is fingerprinted when loaded. Changing a CSV changes the
evidence-package fingerprint.

## 15. Validate the package before sizing

Run the strict parser from the repository root:

```bash
python3 - <<'PY'
from characterization import load_portable_sizing_inputs

inputs = load_portable_sizing_inputs("workload.yaml", "measurements.yaml")
print("input fingerprint:", inputs.fingerprint)
print("services:", inputs.workload.service_names)
print("windows:", len({
    (row.start, row.end) for row in inputs.measurements.service_intervals
}))
PY
```

This checks, among other things:

- exact manifest version and supported fields;
- required CSV columns and numeric values;
- common non-overlapping service windows;
- unique pod/window identities;
- interval alignment across CSVs;
- histogram grids, monotonicity and `+Inf` counts;
- nondecreasing arrival timestamps with positive trace span;
- complete entry-service attribution for multi-entry latency classes;
- unique capacity-run identities.

Parsing success means the package is structurally valid. It does not guarantee
that the evidence is sufficient for a recommendation; the workflow applies
additional readiness and confidence checks.

## 16. Run Kubesizer

Build the native backend once:

```bash
make native-backend
```

For the fitted-variability simulation workflow:

```bash
./scripts/kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation \
  --max-replicas 12 \
  --workers 4 \
  --json
```

For a compatible CPU-only mean-SLO native workload, analytical evaluation is
also available (for example, the repository's `workload.minimum.yaml`):

```bash
./scripts/kubesizer \
  --workload workload.minimum.yaml \
  --replica-evaluation analytical \
  --max-replicas 12 \
  --json
```

Analytical evaluation is not an escape hatch for incomplete characterization.
Supplied and inferred blocking are retained and unsupported semantics cause a
rejection. The portable fitted-variability workflow's trace arrivals with
processor sharing require simulation; the analytical processor-sharing formula
supports Poisson input, unlimited admission, and pooled or random dispatch.

Percentile SLOs require simulation. Without `--out`, the CLI writes no result
file; JSON is printed to standard output and progress goes to standard error.

## 17. Diagnose common rejections

| Error or finding | Likely cause | Correction |
|---|---|---|
| Observation windows are not aligned | Exporters used different starts, ends or durations | Re-export every interval source on one UTC grid |
| Duplicate service interval identity | Multiple collectors emitted the same pod/window | Deduplicate before aggregation; never double-count CPU |
| `+Inf` count disagrees with requests | Histogram and request counter filters differ | Use the same reporter, service, class, status and window cohort |
| Inconsistent histogram boundaries | Pods/windows use different bucket configurations | Standardize buckets or re-bucket from a compatible source |
| Request-class arrival unavailable | External requests were not class-attributed | Add a bounded operation label at ingress/client boundary |
| CPU demand unavailable | CPU and completed requests could not be joined | Restore namespace/pod attribution and application-container filtering |
| Flow-balance mismatch | Entry, class, edge or retry semantics disagree | Reconcile boundaries and retry counting; check declared call multiplicity |
| CPU throttling too high | CPU demand was measured under a distorted limit | Recollect under a healthy limit/request configuration |
| No controlled stability floor | Missing or insufficient healthy capacity runs | Add at least three independent healthy runs per service/tested count |
| Blocking characterization incomplete | Missing low-load service mean/p50, concurrency declaration, or identifiable residual | Add compatible low-load evidence or direct blocking measurements; switching evaluator does not remove this requirement |
| Multiple call-order variants | Edge data cannot identify sequential versus parallel behavior | Declare `call_execution` explicitly |

## 18. Final handoff checklist

Before treating a recommendation as deployable, confirm:

- [ ] service, namespace and pod mappings are correct;
- [ ] only application-container CPU is included;
- [ ] every service uses the same observation windows;
- [ ] replica counts and application versions stayed fixed;
- [ ] every request class has an external rate and the declared SLO statistic;
- [ ] histogram files contain cumulative buckets and `+Inf`;
- [ ] external latency uses the declared boundary;
- [ ] call direction, multiplicity and execution semantics are documented;
- [ ] positive non-CPU blocking is measured or defensibly inferred;
- [ ] concurrency limits come from configuration or saturation evidence;
- [ ] every service has enough independent healthy capacity runs;
- [ ] forecast multipliers stay within a defensible evidence range;
- [ ] the strict parser succeeds;
- [ ] simulation final validation succeeds before a percentile recommendation
  is called SLO-feasible.

The recommendation remains conditional on the measured software, resource
requests, topology semantics, load envelope and health gates. Recollect the
package after material changes to those conditions.
