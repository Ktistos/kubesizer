# Kubesizer: replica sizing

Kubesizer evaluates replica configurations within explicit bounds and returns
the Pareto set of configurations that meet the SLOs. Each option gives its
replica cost, worst latency-to-SLO ratio and replica count per service.
It runs offline and requires Python, NumPy and PyYAML. Simulation also requires
the [C++ backend](native_backend/README.md).

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

Analytical evaluation accepts mean SLOs under its supported assumptions.
Simulation is the default and can evaluate mean and percentile SLOs. Every
returned simulation option passes final validation with fresh seeds.

Optional portable measurements derive CPU demand, request rates, call counts
and boundary residuals. Capacity evidence establishes a measured replica floor
before replica evaluation. A CPU demand measured in CPU-seconds
per request is converted using the declared CPU capacity; aggregate CPU counters
do not reveal per-request demand variability or a hidden worker-pool limit.

Historical reports and schemas are retained under `docs/history/`.

See [the input guide](WORKLOAD_AND_MEASUREMENT_GUIDE.md),
[the measurement guide](DEVOPS_PORTABLE_MEASUREMENT_GUIDE.md) and
[the optimizer description](OPTIMIZATION_METHODS.md).

The opt-in [experimental layered evaluator](docs/EXPERIMENTAL_ANALYTICAL_SEMANTICS.md)
is available for investigation. Its mean-latency accuracy was lower in the
[retrospective STELAR replay](results/stelar-layered-analytical-20261001/REPORT.md).
