# Kubesizer `workload.yaml` and `measurements.yaml` Guide

This guide describes the **portable evidence workflow**, where a DevOps engineer supplies:

```text
workload.yaml     What traffic means and what must be achieved
measurements.yaml Where the observed workload and capacity data are stored
```

The command is:

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation
```

## Example `workload.yaml`

```yaml
# Static service facts needed to interpret the measurement evidence. Keeping
# them in this file gives the workload one canonical service vocabulary.
services:
  api:
    namespace: production
    cpu_request_cores: 0.5
    cpu_limit_cores: 1.0
    concurrency_limit_per_replica: 32

  auth:
    namespace: production
    cpu_request_cores: 0.25

  database:
    namespace: data
    cpu_request_cores: 1.0
    concurrency_limit_per_replica: 16

  cache:
    namespace: production
    cpu_request_cores: 0.25

# Request classes are logical user operations, such as browsing a catalogue
# or submitting an order. The weights must add up to 1.0.
request_classes:
  browse:
    concurrency_weight: 0.75

    # A browse request always enters through api.
    entry_services:
      api: 1.0

    # One api visit makes, on average, one database call and 0.4 cache calls.
    # Call counts may be greater than one.
    calls:
      api:
        database: 1.0
        cache: 0.4

    # The api performs its child calls one after the other.
    call_execution: sequential

    # The SLO clock starts at the external client.
    latency_boundary: external_client

    # These delays are outside the service CPU queues. They are added once to
    # the end-to-end request latency and do not become service demand.
    latency_components:
      client_to_ingress:
        distribution: deterministic
        mean: 0.010
      ingress_processing:
        distribution: lognormal
        mean: 0.004
        scv: 0.50
      unmodeled_residual:
        distribution: lognormal
        mean: 0.008
        scv: 1.00

    # A percentile SLO requires simulation.
    end_to_end_latency_slos:
      percentile:
        quantile: 0.95
        maximum_seconds: 0.250

  checkout:
    concurrency_weight: 0.25

    # Entry weights for one class must add up to 1.0.
    entry_services:
      api: 1.0

    calls:
      api:
        auth: 1.0
        database: 2.0
      auth:
        database: 0.2

    # Sibling calls from the same parent may overlap in time.
    call_execution: parallel

    latency_boundary: ingress
    latency_components:
      ingress_processing:
        distribution: deterministic
        mean: 0.004

    # Mean SLOs can be evaluated analytically or through simulation, provided
    # the selected analytical model supports all declared semantics.
    end_to_end_latency_slos:
      mean_seconds: 0.400

# Optional workload forecast. Kubesizer sizes for the largest multiplier.
# Keeping 1.0 also represents the measured workload itself.
load_forecast:
  multipliers: [1.0, 1.25, 1.5]

# Maximum permitted use of the confidence-bounded healthy request capacity.
# 0.80 reserves at least 20% headroom relative to the bounded estimate.
sizing_policy:
  maximum_capacity_utilization: 0.80

# Optional non-CPU delay inside a service, such as I/O or lock waiting. CPU is
# released during this period, but an admitted worker/concurrency slot remains
# occupied. Do not include downstream calls or network latency again here.
service_blocking_times:
  database:
    distribution: lognormal
    mean: 0.006
    scv: 1.5

# Optional communication model. Values are one-way seconds.
communication_latency:
  same_node_one_way:
    distribution: deterministic
    mean: 0.0002
  cross_node_one_way:
    distribution: lognormal
    mean: 0.0015
    scv: 0.4
  cross_node_probability: 0.60
```

## What the workload fields mean

| Field | Meaning inside Kubesizer |
|---|---|
| `services` | Canonical modeled services and the execution context under which their evidence was measured. |
| `services.<name>.namespace` | Kubernetes namespace used to validate service/pod attribution. |
| `services.<name>.cpu_request_cores` | CPU reservation used to convert measured CPU-seconds/request into an elapsed execution-time estimate. |
| `services.<name>.cpu_limit_cores` | Optional reported CPU ceiling; it is not treated as guaranteed capacity. |
| `services.<name>.memory_*` | Optional retained measurement context; not used to claim Kubernetes schedulability. |
| `services.<name>.concurrency_limit_per_replica` | Measured/configured finite worker, thread, connection, or admission pool. It is required when the portable simulation path infers a positive blocking residual for that service. |
| `request_classes` | Logical operations whose latencies and SLOs are evaluated separately. |
| `concurrency_weight` | Expected share of external requests belonging to the class. All class weights must sum to `1.0`. It is checked against the measured class mix. |
| `entry_services` | How the class's measured external arrival rate is divided among application entry services. The weights for one class must sum to `1.0`. |
| `calls` | Expected synchronous downstream calls per source-service visit. A value may exceed `1.0`. |
| `call_execution` | Whether sibling calls are performed `sequential`ly or in `parallel`. This changes end-to-end latency and parent concurrency. |
| `latency_boundary` | Where the end-to-end latency clock starts: `ingress` or `external_client`. For an external-client mean SLO with no explicit component, Kubesizer attempts a stability-gated `client_to_entry` mean fit from aligned interval sums. |
| `latency_components` | Explicitly measured delays outside service CPU queues, kept separate to avoid treating them as service demand. Explicit components take precedence over automatic mean inference. |
| `end_to_end_latency_slos` | The mean or p95 latency limit used to decide whether a replica configuration is acceptable. |
| `load_forecast` | Multipliers for future traffic. Kubesizer plans for the largest declared value. |
| `sizing_policy.maximum_capacity_utilization` | Maximum fraction of confidence-bounded healthy request capacity that the stability floor may use. It must be greater than zero and less than one; the default is `0.85`. |
| `service_blocking_times` | Optional direct evidence override for non-CPU wait that still occupies a service concurrency slot. Otherwise characterization requires an identifiable low-load residual for every non-negligible service component. Production model construction fails closed when that fit is incomplete; analytical evaluation preserves blocking and rejects unsupported semantics rather than deleting it. |
| `communication_latency` | Same-node and cross-node network-delay distributions plus the measured or estimated cross-node call probability. |

Automatic boundary fitting is deliberately mean-only. It requires at least
three healthy, aligned external/entry windows; completion counts must agree
within 5% or one request in at least 80% of candidate windows; and the retained
window residuals must be stable. The fitted deterministic `client_to_entry`
component is added once per external request in both analytical and simulation
mean-latency evaluation. If the boundary cannot be identified, both evaluators
fail closed. A percentile SLO still requires an explicit component distribution
or paired duration samples—Kubesizer never subtracts independently aggregated
percentiles.

### Where are the arrival rates?

In this portable workflow, arrival rates are deliberately **not repeated in `workload.yaml`**.

They come from the normalized files referenced by `measurements.yaml`, especially:

```text
external-intervals.csv
external-arrival-samples.csv
```

For example, `external-intervals.csv` contains measured request counts over aligned time windows:

```csv
start,end,request_class,request_count
2026-08-31T10:00:00Z,2026-08-31T10:05:00Z,browse,9000
2026-08-31T10:00:00Z,2026-08-31T10:05:00Z,checkout,3000
```

The corresponding observed rates are 30 browse requests/second and 10 checkout requests/second. The measured shares, 0.75 and 0.25, agree with the declared concurrency weights.

This separation prevents a manually typed rate and a measured rate from silently disagreeing.

`external-arrival-samples.csv` provides request-start ordering rather than a
second rate declaration. For a declared latency SLO it is required: Kubesizer
rate-rescales and block-bootstraps the measured trace, preserves equal
timestamps as batches, and rejects missing or ambiguous trace evidence instead
of substituting exponential arrivals. The same file may be supplied as
`external-arrival-samples.csv.gz`. Multi-entry request classes add an
`entry_service` column so each stream receives only its own arrivals. Each
stream needs at least 30 interarrival gaps; production evidence should be much
longer when burst cycles are slow.

Trace blocks contain a fixed number of consecutive gaps, with circular wrapping
at the trace end. `block_duration` is a target expected duration: the gap count
is `ceil(block_duration / mean_gap)`, bounded between one and the trace's gap
count. Uniformly choosing block starts preserves the empirical mean offered
load; it does not force an identical request count in every finite simulation.

## Service declarations inside `workload.yaml`

A minimal portable workload starts with:

```yaml
services:
  api:
    namespace: production
    cpu_request_cores: 0.5

  auth:
    namespace: production
    cpu_request_cores: 0.25

  database:
    namespace: data
    cpu_request_cores: 1.0

  cache:
    namespace: production
    cpu_request_cores: 0.25
```

The service entries can retain more resource context and explicit concurrency limits:

```yaml
services:
  api:
    namespace: production
    cpu_request_cores: 0.5
    cpu_limit_cores: 1.0
    memory_request_bytes: 536870912
    memory_limit_bytes: 1073741824
    concurrency_limit_per_replica: 32

  auth:
    namespace: production
    cpu_request_cores: 0.25
    cpu_limit_cores: 0.5
    memory_request_bytes: 268435456
    memory_limit_bytes: 536870912
    concurrency_limit_per_replica: 16

  database:
    namespace: data
    cpu_request_cores: 1.0
    cpu_limit_cores: 2.0
    memory_request_bytes: 2147483648
    memory_limit_bytes: 4294967296
    concurrency_limit_per_replica: 64

  cache:
    namespace: production
    cpu_request_cores: 0.25
    cpu_limit_cores: 0.5
    memory_request_bytes: 268435456
    memory_limit_bytes: 536870912
```

## Why the service facts remain useful

The separate deployment file was redundant, but its service facts were not. They now live under `workload.services`, beside the request topology that refers to them.

### 1. It defines the canonical service set

Prometheus data is reported by namespace and pod, while workload topology uses logical service names. `workload.services` tells Kubesizer which services belong to the experiment and which namespace each service occupies.

This prevents unrelated pods with similar names from being mixed into the model.

### 2. It makes metric joins reproducible

The portable measurement pipeline joins Istio throughput with application-container CPU usage using the normalized identity:

```text
namespace + pod
```

The workload service declarations provide the expected namespace/service ownership used to validate that attribution.

### 3. It turns CPU demand into an execution-time model

From measurements, Kubesizer obtains:

```text
CPU demand per request = CPU seconds consumed / completed requests
```

This is measured in CPU-seconds/request. It is not wall-clock service time.

The CPU request describes the CPU capacity Kubernetes reserves for one replica. Kubesizer can therefore estimate the elapsed CPU execution time under that reservation:

```text
execution time estimate
    = CPU-seconds per request / requested CPU cores
```

Example:

```text
CPU demand       = 0.010 CPU-seconds/request
CPU request      = 0.5 cores
execution estimate = 0.010 / 0.5 = 0.020 seconds/request
```

The CPU limit is retained as a ceiling and diagnostic. Kubesizer does not treat a limit as guaranteed capacity under contention.

### 4. It can model finite application concurrency

`concurrency_limit_per_replica` represents a measured or configured worker, thread, connection-pool, or admission limit.

This matters when a request performs CPU work and then waits for I/O or a downstream service. The CPU may be free, but the request can still occupy one of the application's finite slots. Without this field, the simulator cannot represent that particular bottleneck explicitly.

The field can be omitted when no positive blocking residual is inferred.
If portable characterization identifies a positive
residual, Kubesizer rejects the model until the limit is supplied; it does not
silently invent a worker-pool size. A directly supplied blocking distribution
can still represent additive I/O delay without a finite limit, but then it does
not claim a finite worker-pool bottleneck.

Analytical evaluation does not discard either supplied or inferred blocking to
obtain a CPU-only prediction. The current analytical workflow rejects workloads
with blocking; use simulation with complete characterization instead.

An independently measured delay after releasing the service's own admission
slot can instead be supplied as `service_post_release_delays.<service>` in the
portable workload. It becomes `traffic_model.service_times.<service>.post_release_delay`
in the frozen simulation model. It adds response time without occupying that
service's CPU or slot; a synchronous caller still waits. It is not an automatic
split of response latency and must not overlap network or client residuals.
See the [measurement instructions](DEVOPS_PORTABLE_MEASUREMENT_GUIDE.md#optional-delay-after-releasing-the-services-slot).
Analytical sizing currently rejects this feature. Omitting it preserves the
previous semantics; no new measurements are silently invented.

For CPU-only workloads, Poisson arrivals with unlimited processor-sharing
admission use an insensitive mean formula matching the native kernel. Random
replica dispatch is evaluated per replica, not as a shared queue. Fixed-slot
round-robin dispatch uses a per-replica renewal-moment approximation, explicitly
not an exact Poisson result. Non-Poisson or finite-admission processor sharing,
including processor sharing with round-robin dispatch, requires simulation.

### 5. It records the resource conditions under which measurements are valid

A service-demand estimate is not universal. It was observed under a particular CPU reservation, memory configuration, software version, node environment, and workload.

Keeping these facts in the workload fingerprint makes it possible to detect that the measured execution context changed and that old evidence should not be reused blindly.

### 6. It does not tell Kubesizer the answer

`workload.services` describes one replica's measured execution context. It does not provide the recommended replica count. The replica count is the output of the stability and SLO optimization.

Deployment CPU and memory values characterize the execution environment in
which the evidence was collected. They do not drive node assignment, resource
packing, or a Kubernetes scheduling proof.

## Minimum practical portable package

For a deployment-informed recommendation, provide at least:

```text
workload.yaml
  - every modeled service
  - namespace and CPU request for every service
  - optional resource context
  - concurrency limits for services whose non-CPU blocking is modeled
  - every request class
  - concurrency weight for every class
  - entry-service weights
  - calls and explicit call execution semantics
  - a mean or p95 end-to-end latency SLO
  - optional maximum capacity utilization; default 0.85

measurements.yaml and retained CSVs
  - aligned external request-class counts
  - aligned per-service/pod completed requests and CPU seconds
  - class-specific service counts or service-edge evidence
  - independent healthy fixed-replica capacity runs
```

Response-duration sums provide the request-weighted mean, while latency
histograms provide the p50 needed by fitted variability. Histogram buckets must
be unique, cumulative, use one boundary grid per subject, end in `+Inf`, and
have a final count that matches the corresponding completed-request count.
Arrival traces, network measurements, direct blocking-time evidence, errors,
throttling, memory, and concurrency limits enrich or constrain the model when
they can be extracted reliably.

## Key distinction

The easiest way to remember the files is:

> `workload.yaml` describes the services, workload, and sizing question.
>
> `measurements.yaml` points to the evidence observed for that workload.
>
> Kubesizer produces the replica answer.

Kubesizer does not require a node-resource file and does not produce a node
assignment. Apply the resulting replica counts through the normal Kubernetes
scheduler and deployment policies.
