# Optimization methods and guarantees

Kubesizer has two decision stages: replica sizing chooses a replica count for
each service, and node placement distributes those fixed replicas across
nodes. Both expose EXACT, SCALABLE, and AUTO modes. AUTO uses an estimated
finite search-space size and a configurable threshold. Results record why a
method was selected.

The scalable methods are heuristics. They have no general approximation ratio
and must not be described as globally, approximately, or near optimal.

## Feasibility semantics

Heuristic search normalizes evaluator output to:

- FEASIBLE: deterministic or statistical evaluation certified the candidate.
- INFEASIBLE: evaluation conclusively rejected the candidate.
- UNKNOWN: intervals overlap an SLO, sampling is incomplete, or evaluation
  could not make a valid decision.

Statistical classification takes precedence over compatibility booleans.
INCONCLUSIVE is always UNKNOWN, never feasible. Immutable canonical
configurations are cached. A cached candidate is evaluated again only when the
problem explicitly supports additional statistical sampling.

## Replica sizing

### Exact mode

UniformCostReplicaOptimizer searches in nondecreasing weighted
replica-addition cost. The calibrated problem starts at the componentwise
minimum analytically stable vector. When every lower-cost state is conclusive,
it proves minimum weighted replica cost inside the configured bounds. Unknown
lower-cost states prevent that proof and are reported.

### Scalable mode

MultiStartReplicaVNSOptimizer is also exported through the compatible name
ScalableReplicaOptimizer. Each restart performs:

1. Randomized greedy construction
   - Start at the analytical stability vector.
   - Rank valid single-replica additions analytically or with a supplied ranker.
   - Form a restricted candidate list and select with a local seeded generator.
   - Evaluate selected candidates with the authoritative evaluator.
   - Analytical ranking never certifies SLO feasibility.

2. Deletion descent
   - Test every permitted one-replica removal.
   - Never cross below the stability vector.
   - Accept a certified feasible removal and restart the scan.
   - Claim deletion-local certification only after a complete final pass where
     every remaining removal is conclusively infeasible.
   - Unknown removals or budget interruption invalidate the certificate.

3. Add-and-reprune VNS
   - Add one, two, or three replicas using ranking and diversification.
   - Re-run deletion descent over all services.
   - Accept only a certified feasible lower-cost result.
   - The one-add neighborhood can perform an exchange indirectly.

4. Multi-start retention
   - Repeat with deterministic seeded choices.
   - Retain the lowest weighted-cost certified feasible incumbent.
   - Use SLO slack only as an equal-cost secondary tie-breaker.
   - Never replace an incumbent with a more expensive result.

If analytical stability is necessary for every feasible configuration, the
weighted stable-vector cost is a valid basic lower bound. For incumbent U and
valid L greater than zero, the certified relative bound is (U-L)/L. It may be
loose. The heuristic still reports global_optimal false.

ScalableReplicaSearchOptions controls unique and total call budgets, time,
candidate width and score fraction, restart and no-improvement limits, VNS
size and attempts, seed, removal order, equal-cost diversification, and trace
collection.

For n services, a construction step ranks at most n additions and a deletion
pass tests at most n removals. Simulation usually dominates this polynomial
neighborhood work. Restarts and VNS are bounded by evaluation and time limits.

## Node placement

### Exact mode

BranchAndBoundNodePlacementOptimizer assigns service distributions and prunes
only on hard infeasibility or an admissible objective bound. If it finishes,
it proves the global minimum for the modeled placement objective.

### Scalable mode

BeamVNSPlacementOptimizer is also exported as
ScalableNodePlacementOptimizer. It performs:

1. Diversity-aware per-replica beam construction
   - Order replicas using resource demand, communication degree, fit scarcity,
     and spreading relevance.
   - Expand each partial state to every capacity-feasible node.
   - Enforce CPU, memory, and an optional hard-constraint callback.
   - Score committed network and spreading contributions.
   - Limit duplicate coarse resource-load signatures.
   - An empty beam means only that the restart found no placement; it is not
     proof that the complete problem is infeasible.

2. Relocation descent
   - Scan every feasible one-replica relocation.
   - Use exact incremental network and spreading deltas.
   - Apply the best strict improvement and restart the scan.

3. Pairwise-swap descent
   - Scan replicas on different nodes.
   - Check capacity and hard constraints after the swap.
   - Apply the best strict improvement and return to relocation descent.

4. Small LNS repair
   - Select small high-pressure and randomized replica subsets.
   - Remove and jointly reassign them with a bounded mini-beam.
   - Accept only complete feasible strict improvements.
   - Re-run relocation and swap descent after improvement.

5. Multi-start and final polishing
   - Use several seeded replica orders and randomized ties.
   - Retain the cheapest feasible incumbent.
   - Run a final relocation/swap scan if budget remains.

A result is relocation-and-swap local only when complete final scans of both
neighborhoods find no improvement. LNS does not claim q-neighborhood
optimality because it examines bounded subsets.

ScalablePlacementSearchOptions controls beam width and diversity, partial and
complete budgets, total calls, time, restarts, no-improvement limit, seed,
local iterations, LNS sizes, subset attempts, subproblem budget, and traces.

For R replicas and N nodes, construction expands at most beam_width times N
states per replica. Relocation is O(RN), swap is O(R squared), and incremental
deltas inspect affected communication edges. LNS work has a strict cap.

The exact relaxation is exposed as a placement lower bound when valid. A zero
lower bound produces an absolute gap but no relative certificate.

## Result metadata

Exact and scalable results expose, where meaningful:

- method, selected-mode reason, objective, and configuration;
- three-state feasibility and termination reason;
- evaluator calls, unique configurations, and simulation replications;
- runtime, restart count, seed, and budget status;
- deletion, relocation, and swap local certificates;
- global-optimality flag;
- valid lower bound and absolute or relative certified gaps;
- optional trace or improvement history.

No-feasible-solution-found means no certified solution was found within the
heuristic budget. It is not proof of mathematical infeasibility.

## Pipeline boundary

The native simulator consumes the selected replica placement, directional
one-way node latencies, and optional measured node execution-time multipliers.
Synchronous service calls include both request and response network delay.
When simulation-based sizing and placement are both requested, the fresh-seed
final SLO validation runs after placement so it certifies the combined plan.

Replica sizing accepts an optional structural-feasibility callback so
aggregate or downstream placement limits can reject impossible replica vectors
without coupling both optimizers.

## Reproducibility and non-guarantees

All randomized choices use a local deterministic seed and do not alter
simulation streams. Exact and scalable modes share the authoritative evaluator.

Honest interpretations are:

- replica heuristic: certified feasible and possibly deletion-local;
- placement heuristic: hard-constraint feasible and possibly
  relocation-and-swap local;
- neither heuristic has a general global or approximation guarantee;
- numerical gaps are certificates only when the lower bound is valid;
- observed test gaps are empirical evidence, not worst-case guarantees.
