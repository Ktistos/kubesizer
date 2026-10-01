# Collecting portable measurements for analytical sizing

1. Record each service's deployed replica count, CPU capacity and request
   routing. Specify whether child calls execute sequentially or concurrently.
   Record worker/concurrency limits from deployment configuration when known.
2. Choose common, non-overlapping UTC windows. Keep topology and replica counts
   fixed within each window. Record restarts, CPU throttling and request failures.
3. Collect application CPU usage and completed request counts per replica from
   Prometheus and mesh/application telemetry. Derive service CPU-seconds/request
   from total CPU-seconds divided by total completions, using aligned windows.
4. Collect per-service and external request counts, response-time sums and
   histograms, and per-class dependency-edge counts from Istio or tracing.
   Capture request-start timestamps when available to characterize arrival SCV.
5. Include healthy low-load windows to characterize local work and stable
   boundary latency. External mean minus entry-service mean can estimate a
   client-to-entry residual when the request populations and boundaries match.
6. If planning beyond observed load, provide repeated capacity observations to
   establish the evidence-based replica floor. Observed maximum active requests
   does not by itself identify a worker/concurrency limit.
7. Place the CSV paths in `measurements.yaml` using methodology version
   `prometheus-istio-kubernetes-v1`; start from `examples/portable_evidence/`.
8. Run `./scripts/kubesizer --workload workload.yaml --measurements measurements.yaml --json`.
   Inspect unsupported-semantics or evidence errors before interpreting a result.

Per-request service variance is not recoverable from aggregate CPU counters.
Explicit execution distributions require independent per-request observations
or a stated modeling assumption. The analytical result is a mean point estimate;
it has no simulated percentile or measured SLO confidence guarantee.

The [historical collection guide](docs/history/DEVOPS_PORTABLE_MEASUREMENT_GUIDE.md)
contains detailed CSV schemas and collection examples. Its simulation-specific
steps apply to the preserved simulation branch.
