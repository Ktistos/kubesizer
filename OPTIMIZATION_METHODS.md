# Replica optimization method and guarantees

Kubesizer chooses the replica count for each modeled service. The CLI returns
the configurations that trade off replica cost against latency headroom.

## Cost and latency frontier

The CLI evaluates the replica grid between the stability floor and the
configured per-service upper bounds. The analytical stability floor applies
to a directly modeled workload. Portable measurement input uses the
confidence-bounded stability floor. Each replica costs one unit in the CLI.

A configuration dominates another when it costs no more and its worst latency
ratio is no higher, with at least one strict improvement. The ratio is the
highest latency divided by its SLO across request classes. Analytical
evaluation uses predicted means. Simulation uses the upper confidence bounds
computed during the search. Equal objective values keep both configurations.
An analytical workload without latency SLOs uses stability and cost alone.

## Feasibility and validation

Analytical evaluation gives a conclusive feasible or infeasible result under
the supported mean-latency model. Simulation may also be statistically
inconclusive. The result reports how many search evaluations remained
inconclusive.

Simulation independently validates each option using fresh seeds.
The final confidence budget accounts for all search-feasible candidates and
their SLO constraints. A candidate that fails validation leaves other
candidates eligible for the frontier. The displayed latency ratio still
comes from the search. If every candidate fails, the CLI returns an error.

These bounds and decisions apply to the declared model and replica grid.
Inconclusive candidates can contain other trade-offs. The Python
single-plan workflow still uses `UniformCostReplicaOptimizer` and reports
whether the minimum cost was established under its model.

## CLI result

The JSON envelope contains `result.pareto_set` in increasing cost order.
Each option has `replica_cost`, `worst_slo_ratio`, and `services`. Each
service contains `recommended_replicas`. The result also identifies the
latency basis and counts failed final validations. With stability-only sizing
the latency basis and ratio are null.

## Runtime consequence

For service `i`, let `L_i` and `U_i` be its lower and upper replica bounds. The
bounded space contains

```text
product over i of (U_i - L_i + 1)
```

configurations. Frontier search evaluates all of them for workloads with
latency SLOs. Simulation can require additional replications for final
validation. Use `--max-replicas` to restrict the search to a practical size.
