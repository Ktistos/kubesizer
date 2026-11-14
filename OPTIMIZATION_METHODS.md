# Replica optimization method and guarantees

Kubesizer makes one optimization decision: the replica count for each modeled
service. It does not optimize Kubernetes node placement or claim that a
recommendation is schedulable on a particular cluster.

## One exact search method

`UniformCostReplicaOptimizer` is the only optimizer. There is no search-mode
option, randomized approximation, multi-start search, VNS, or automatic switch
to a cheaper method.

The search begins at the hard per-service lower bound: analytical stability for
a directly modeled workload, or the confidence-bounded statistical stability
floor for portable measurement input. A state is a replica vector. Each edge
adds one replica, and its edge cost is that service's configured replica cost.
The priority queue therefore visits states in nondecreasing total added cost.
Analytical utilization may order states of exactly equal cost, but it cannot
change which cost level is searched first.

## Feasibility semantics

Analytical evaluation gives a conclusive feasible or infeasible result under
the supported mean-latency model. Simulation evaluation may also be
statistically inconclusive. An inconclusive state is never accepted as
feasible.

Search stops after it has evaluated every reachable state whose cost is no
greater than the best feasible state's cost. If all cheaper states were
conclusively infeasible, the result is the minimum weighted replica cost inside
the declared bounds. If a cheaper state was statistically inconclusive, the
returned vector is feasible but minimum cost is explicitly reported as not
proven.

Fresh final validation is separate from optimization in simulation mode. A
configuration is emitted by the CLI only if that validation passes.

## Result metadata

The exact result records:

- the replica vector, weighted objective, explored-state count, and search path;
- whether bounded-model minimality was proven;
- any statistically unresolved cheaper configurations;
- simulation replication count and runtime;
- the valid lower bound and absolute or relative bound gap.

“Exact” applies only to the finite declared replica bounds and the selected
analytical or frozen simulation model. It is not a claim that the model captures
unmeasured deployment behavior, and it is not a Kubernetes scheduling result.

## Runtime consequence

For service `i`, let `L_i` and `U_i` be its lower and upper replica bounds. The
bounded space contains

```text
product over i of (U_i - L_i + 1)
```

configurations. Exact search may need to evaluate all of them. The runtime
preflight reports this full ceiling; large bounds must be reduced by the user
instead of silently changing optimization semantics.
