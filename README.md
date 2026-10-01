# Kubesizer: analytical replica sizing

Kubesizer estimates service utilization and mean request latency, then finds
the lowest-cost replica configuration within explicit bounds using exact
uniform-cost search. It runs offline and requires Python, NumPy and PyYAML.

```sh
python -m pip install -r requirements.txt
./scripts/kubesizer --workload workload.minimum.yaml --json
./scripts/kubesizer --workload examples/portable_evidence/workload.yaml \
  --measurements examples/portable_evidence/measurements.with-capacity.yaml --json
make test
```

The evaluator selects M/M/c or M/G/1 when the assumptions permit an exact local
mean; M/G/c, G/M/c and G/G/c use variability-adjusted Erlang-C approximations.
Arrival and service distributions supply means and squared coefficients of
variation. Network arrival variability is propagated approximately. Explicit
latency components, including client-to-entry residuals, contribute their means.
Exact replica search does not make an approximate latency formula exact.

The baseline accepts mean SLOs and rejects unsupported blocking/held-concurrency
semantics, percentile SLOs and end-to-end deadlines. Direct mean calculations
remain available for exploratory diagnostics. Simulation, native compilation,
node placement and scalable search are absent from this version.

Optional portable measurements derive CPU demand, request rates, call counts
and boundary residuals. Capacity evidence establishes a measured replica floor
before the analytical optimizer searches. A CPU demand measured in CPU-seconds
per request is converted using the declared CPU capacity; aggregate CPU counters
do not reveal per-request demand variability or a hidden worker-pool limit.

The `feature/statistical-simulation-workflow` branch preserves simulation work.
Historical reports and schemas are retained under `docs/history/`; their
simulation commands refer to that branch.

See [the input guide](WORKLOAD_AND_MEASUREMENT_GUIDE.md),
[the measurement guide](DEVOPS_PORTABLE_MEASUREMENT_GUIDE.md) and
[the optimizer description](OPTIMIZATION_METHODS.md).
