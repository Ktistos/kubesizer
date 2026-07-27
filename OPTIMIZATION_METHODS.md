# Optimization Methods and Objective Definitions

This project currently implements two optimization methods with different
decision variables. Replica sizing changes replica counts. Node placement
keeps those counts fixed and changes only their node assignments.

## 1. A* replica sizing

Implementation:

- Algorithm: `AStarOptimizer` in `optimization/astar.py`
- Problem: `ReplicaSizingProblem` in `optimization/replica_sizing.py`
- Default heuristic: `AnalyticalStabilityHeuristic`

### State

A state is a replica-count tuple:

```text
c = (c_0, c_1, ..., c_n)
```

The initial state contains one replica per service. A transition increments
exactly one service by one replica, subject to its configured maximum.

### Search cost

Every valid transition has cost one:

```text
step_cost(current, neighbor) = 1
```

Therefore the A* path cost is:

```text
g(c) = sum_i(c_i - 1)
```

This is the number of replicas added from the all-ones initial state. The
`OptimizationResult.cost` field contains this path cost.

A feasible `ReplicaSizingEvaluation.cost` is `sum_i(c_i)`, the total number
of replicas. The two values differ by the constant number of services, so they
produce the same replica-count ordering.

### Feasibility and tie-breaking

A replica configuration is feasible only when:

1. The analytical queueing model is stable.
2. Every configured request-class p95 latency satisfies its SLO.

Among equal path-cost goals, A* uses:

```text
tie_breaker = max_k(measured_p95_k / slo_k)
```

A smaller value provides more latency headroom. This tie-breaker does not
change the primary minimum-replica objective.

### Default heuristic

For service `i`, with effective arrival rate `lambda_i`, per-replica
service rate `mu_i`, and current replicas `c_i`, strict analytical
stability requires:

```text
required_i = floor(lambda_i / mu_i) + 1
```

The heuristic is:

```text
h(c) = sum_i max(0, required_i - c_i)
```

Each transition can reduce this deficit by at most one, so the heuristic is a
lower bound on the number of remaining scale-up steps and is admissible.

The alternative `zero_heuristic` returns zero for every state and reduces A*
to uniform-cost search.

## 2. Exact fixed-replica node placement

Implementation:

- Algorithm: `ExhaustiveNodePlacementOptimizer` in
  `optimization/exhaustive_node_placement.py`
- Problem and cost: `NodePlacementProblem` in
  `optimization/node_placement.py`

### State

The integer decision variable is:

```text
x[i,u] = replicas of service i placed on node u
```

The external dictionary representation is node-oriented:

```python
{
    "node-a": {"api": 2, "worker": 1},
    "node-b": {"api": 1},
}
```

Zero entries may be omitted.

### Hard constraints

Every candidate must satisfy:

```text
sum_u x[i,u] = c_i                    for every service i
x[i,u] is a nonnegative integer
```

Here `c_i` is the fixed workload replica count. The algorithm never creates
scale-up or scale-down candidates.

When complete per-service Kubernetes requests are supplied:

```text
sum_i x[i,u] * cpu_request_i    <= allocatable_cpu_u
sum_i x[i,u] * memory_request_i <= allocatable_memory_u
```

CPU and memory constraints are omitted when requests are unavailable, and the
evaluation reports `resource_feasibility_checked: false`. Invalid candidates
receive infinite cost and cannot become an optimum.

### Derived traffic

For external arrivals `gamma` and expected downstream call matrix `P`:

```text
lambda = gamma + transpose(P) * lambda
lambda = inverse(I - transpose(P)) * gamma
T[i,j] = lambda_i * p[i,j]
```

Route values are used directly as expected call counts. They are not
normalized as mutually exclusive probabilities. Arrival rates and edge
traffic are calculated once when the placement problem is constructed.

### Network component

Under uniform load balancing across replicas:

```text
C_network(x) =
    sum_(i,j) T[i,j]
    * sum_(u,v) (x[i,u] / c_i) * (x[j,v] / c_j) * d[u,v]
```

Its unit is accumulated network-delay seconds per second.

Average network delay per external request is:

```text
D_network_average(x) = C_network(x) / sum_i gamma_i
```

The dimensionless normalized network cost is:

```text
C_network_normalized(x) =
    C_network(x) / (d_max * sum_(i,j) T[i,j])
```

It is defined as zero when the denominator is zero.

### Replica-spreading component

For every service with more than one replica:

```text
H_i(x) = sum_u (x[i,u] / c_i)^2

q_i = floor(c_i / N)
r_i = c_i mod N

H_i_min =
    (r_i * (q_i + 1)^2 + (N - r_i) * q_i^2) / c_i^2

P_i(x) = (H_i(x) - H_i_min) / (1 - H_i_min)
```

The overall penalty is the equal-weight mean:

```text
P_spread(x) = mean_i P_i(x), for services with c_i > 1
```

A single-replica service contributes no spreading penalty. A service that is
as evenly distributed as its integer count permits has penalty zero; putting
all its replicas on one node has penalty one.

### Total placement cost

```text
C_total(x) =
    alpha * C_network_normalized(x)
    + beta * P_spread(x)
```

The defaults are:

```yaml
network: 0.7
replica_spreading: 0.3
```

Both weights are configurable finite nonnegative numbers.

### Exact enumeration and heuristic

For `c_i` identical replicas of service `i` across `N` nodes, the number
of integer distributions is:

```text
combinations(c_i + N - 1, N - 1)
```

The complete search space is:

```text
product_i combinations(c_i + N - 1, N - 1)
```

For the current counts `(3, 2, 5, 2, 2)` and three nodes, this is 45,360
placements.

The optimizer evaluates every placement, rejects infeasible ones, and returns
the feasible candidate with the smallest `C_total`. This guarantees the
global optimum for the enumerated model.

No heuristic is used. Exact enumeration was selected because the current
space is small, the objective contains pairwise quadratic placement terms,
and this approach requires no external mixed-integer solver. If the state
space later becomes too large, the same problem definition can be paired with
branch-and-bound, MIQP, or a documented approximate search.
