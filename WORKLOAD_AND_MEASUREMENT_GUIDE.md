# Kubesizer `workload.yaml` and `measurements.yaml` Guide

This guide describes the **portable evidence workflow**, where a DevOps engineer supplies:

```text
workload.yaml     What traffic means and what must be achieved
measurements.yaml Where the observed workload and capacity data are stored
resources.yaml    Optional node communication data for placement
```

The command is:

```bash
kubesizer \
  --workload workload.yaml \
  --measurements measurements.yaml \
  --replica-evaluation simulation \
  --replica-search auto \
  --skip-placement
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

# Optional pre-placement communication model. Values are one-way seconds.
communication_latency:
  same_node_one_way:
    distribution: deterministic
    mean: 0.0002
  cross_node_one_way:
    distribution: lognormal
    mean: 0.0015
    scv: 0.4
  unplaced_cross_node_probability: 0.60
```

## What the workload fields mean

| Field | Meaning inside Kubesizer |
|---|---|
| `services` | Canonical modeled services and the execution context under which their evidence was measured. |
| `services.<name>.namespace` | Kubernetes namespace used to validate service/pod attribution. |
| `services.<name>.cpu_request_cores` | CPU reservation used to convert measured CPU-seconds/request into an elapsed execution-time estimate. |
| `services.<name>.cpu_limit_cores` | Optional reported CPU ceiling; it is not treated as guaranteed capacity. |
| `services.<name>.memory_*` | Optional retained measurement context; not currently a placement-packing proof. |
| `services.<name>.concurrency_limit_per_replica` | Optional measured/configured finite worker, thread, connection, or admission pool. |
| `request_classes` | Logical operations whose latencies and SLOs are evaluated separately. |
| `concurrency_weight` | Expected share of external requests belonging to the class. All class weights must sum to `1.0`. It is checked against the measured class mix. |
| `entry_services` | How the class's measured external arrival rate is divided among application entry services. The weights for one class must sum to `1.0`. |
| `calls` | Expected synchronous downstream calls per source-service visit. A value may exceed `1.0`. |
| `call_execution` | Whether sibling calls are performed `sequential`ly or in `parallel`. This changes end-to-end latency and parent concurrency. |
| `latency_boundary` | Where the end-to-end latency clock starts: `ingress` or `external_client`. |
| `latency_components` | Measured delays outside service CPU queues, kept separate to avoid treating them as service demand. |
| `end_to_end_latency_slos` | The mean or p95 latency limit used to decide whether a replica configuration is acceptable. |
| `load_forecast` | Multipliers for future traffic. Kubesizer plans for the largest declared value. |
| `sizing_policy.maximum_capacity_utilization` | Maximum fraction of confidence-bounded healthy request capacity that the stability floor may use. It must be greater than zero and less than one; the default is `0.85`. |
| `service_blocking_times` | Non-CPU wait that still occupies a service concurrency slot. This requires the rich simulation semantics. |
| `communication_latency` | Same-node and cross-node network-delay distributions before a concrete placement is selected. |

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

The field is optional because it is not reliably available for every application. If it is omitted, Kubesizer must use the less specific service model and the limitation should be acknowledged.

### 5. It records the resource conditions under which measurements are valid

A service-demand estimate is not universal. It was observed under a particular CPU reservation, memory configuration, software version, node environment, and workload.

Keeping these facts in the workload fingerprint makes it possible to detect that the measured execution context changed and that old evidence should not be reused blindly.

### 6. It does not tell Kubesizer the answer

`workload.services` describes one replica's measured execution context. It does not provide the recommended replica count. The replica count is the output of the stability and SLO optimization.

It also does not replace `resources.yaml`:

| File | Scope |
|---|---|
| `workload.yaml` | Services, request topology, SLOs, sizing policy, and measurement context |
| `resources.yaml` | Optional node identities, node-to-node latency, and execution multipliers for placement |

The current CLI node-placement flow does not enforce CPU/memory packing because it does not yet pass per-service placement demands into the placement problem. Therefore deployment CPU/memory values currently characterize service execution; they are not a Kubernetes scheduling proof.

## Minimum practical portable package

For a deployment-informed recommendation, provide at least:

```text
workload.yaml
  - every modeled service
  - namespace and CPU request for every service
  - optional resource context and concurrency limits
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

Latency histograms, arrival traces, network measurements, blocking-time evidence, errors, throttling, memory, and concurrency limits enrich the model when they can be extracted reliably.

## Key distinction

The easiest way to remember the files is:

> `workload.yaml` describes the services, workload, and sizing question.
>
> `measurements.yaml` points to the evidence observed for that workload.
>
> `resources.yaml` optionally describes nodes for placement.
>
> Kubesizer produces the replica answer.
