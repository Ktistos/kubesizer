# Analytical sizing and guarantees

The only optimizer is bounded uniform-cost search over replica counts. Node
placement, scalable search approximations, and simulation are not included.

Each transition adds one replica at its configured positive cost. Search starts
at analytical stability, or a confidence-bounded CPU capacity floor when portable
measurements are provided. It checks all states no more expensive than the best
feasible state before claiming minimum cost within the declared bounds.

Queueing evaluation retains M/M/c, M/G/1, and variability-adjusted G/G/c models.
Only the first two have exact mean formulas under their assumptions; exact
replica search does not make the other queueing formulas exact.

Fixed boundary residuals and declared communication means are included in the
end-to-end mean. Unsupported blocking/parent-holding semantics, terminal
timeouts, and percentile SLOs are rejected by the baseline sizing workflow.
Direct diagnostic calls to System.solve() do not certify unsupported workloads.

For lower bounds L and upper bounds U, the search space contains the product of
U[i] - L[i] + 1. There is no automatic fallback when this space is large.

Recommendations are conditional model predictions, not deployment guarantees.
They do not prove Kubernetes schedulability or rule out unmeasured bottlenecks.
