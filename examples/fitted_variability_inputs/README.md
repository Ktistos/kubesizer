# Fitted-variability experiment input skeletons

These compact files document the inputs used by the historical rich-campaign
fitted-variability method. They use the `same_node_chain` names and representative
values so the fields can be followed across the pipeline. They are explanatory
skeletons, not a replacement for the campaign's hash-bound raw artifacts.

## Input boundary

Portable measurement evidence, repeated for five independent low-load runs:

- `portable-measurement.yaml`: service CPU demand and response-time histogram
  statistics collected from Kubernetes/Prometheus/Istio;
- `portable-request-cohort.yaml`: completed client cohort, rate, failures and
  end-to-end latency collected by the load generator;
- `portable-config.yaml`: the measurement window and request-class mapping.

Portable planning contract (required, but not measured):

- `planning-contract.yaml`: service topology, call order, SLO, entry service,
  CPU capacity/model form, routing assumption and headroom policy;
- `experiment-design.yaml`: forecast load and candidate replica configurations.

Application declaration (portable as an operator-supplied contract, but not
inferable from Prometheus/Istio counters):

- `nonportable-concurrency.yaml`: historically named file containing finite
  per-replica admission/worker capacity. The retained fitted model used these
  values; the current production path requires an equivalent service
  declaration whenever it infers positive blocking.

The original synthetic `family-*.yaml` files also contained the generator's
true CPU and blocking distributions. Those were privileged oracle data. The
fitted-variability parameter calculation did not use them, so they are omitted
from these input skeletons instead of being mixed with legitimate fields.

## Transformation

For each service, the method computes:

```text
CPU demand = application CPU seconds / completed service requests

blocking mean = observed service response mean
                - measured CPU demand
                - modeled downstream response mean
```

For a leaf service, its response-time p50 supplies a residual median. When that
median is below the blocking mean, the frozen fit is:

```text
sigma_squared = 2 * log(blocking_mean / blocking_median)
SCV = exp(sigma_squared) - 1
```

The direct generated model examples are:

- portable-only:
  `results/portable-measurement-replay-20260908/sanitized-models/`
  `same_node_chain.portable_fitted_variability.yaml`;
- historical concurrency-aware:
  `STELAR_LOAD_TESTS/experiments/runner-audit-20260907/campaign/`
  `follow-up/hardening/corrected-frozen/calibrated_models/`
  `same_node_chain.telemetry_blocking_fitted_variability.yaml`.

The important behavioral difference between those two generated examples is
the presence of `concurrency_limit_per_replica` on each service.
