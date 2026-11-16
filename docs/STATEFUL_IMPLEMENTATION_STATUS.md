# Stateful modeling branch status

Branch: `feature/stateful-performance-models`, based on `199eb2b`.
Local implementation and verification: 2026-11-16.

## Completed

- A sourced inventory of 16 reference patterns, organized by mechanisms rather
  than a purported exhaustive count of database architectures.
- Per-source sequential/parallel/exclusive routing and all/quorum/background
  completion in the native C++ executor. Remaining branches keep consuming
  resources after early acknowledgement; late returns retain valid ownership.
- Fixed/bounded components in portable initial sizing, native configuration,
  calibration bounds and exact replica search.
- An analytical opt-in for conditional mean completion bounds. Uncertified
  point means are not substituted for quorum response. Upper-bound failure
  alone is inconclusive and cannot establish a proven minimum-cost plan.
- Finite topology comparison through `--stateful-candidates`, available with
  either analytical or simulation evaluation. Every candidate supplies its
  own topology/demand evidence and physical cost; external traffic/SLOs must
  agree. Evidence is hashed and checked for changes before and after scoring.
- Synthetic examples for replication, shard routing, scatter/gather, quorum,
  shared storage and erasure-like fan-out, plus one/two-follower comparisons.
- CLI version 0.5.0, native protocol 15, semantics v18; calibration identity
  includes source policies, analytical mode and replica constraints.

## Verification

- Tracked repository tests plus new stateful tests: **713 passed**.
- Focused stateful/native/call-graph checks against the ASan/UBSan backend:
  **91 passed**.
- Formatting and patch-whitespace checks passed.
- A local software smoke test used the synthetic `analytical-quorum.yaml`
  input in simulation mode, with reduced software-test calibration settings.
  Calibration, exact sizing and 12 fresh final replications completed;
  all three declared members stayed fixed at one. This is an integration
  check, not measured database performance or deployment reliability evidence.

## Limits and next experimental step

No real database/storage accuracy result is produced by this change. No live
stateful experiment was launched, and the existing comparison VM/release and
frozen campaign were not modified.

Architecture names do not select hard-coded database formulas. Database and
version can appear in provenance, while roles, routing, acknowledgement rules
and measured resource demand determine the model. Valid storage layout changes
must be resolved externally before comparison; node placement remains outside
scope.

Generic Istio/CPU telemetry does not identify replication replay, fragments,
fsync or multi-round commit. New per-source policies currently use explicitly
characterized frozen inputs. Elections, correctness/consistency protocols,
conflicts, state migration, healing, transactions and checkpoint barriers
are not implemented as product protocols. See the inventory for precise gaps.

Next: follow [STATEFUL_EXPERIMENT_PROTOCOL.md](STATEFUL_EXPERIMENT_PROTOCOL.md)
to collect characterization evidence for selected real configurations, freeze
the models and then measure untouched targets. Compare accuracy, abstention,
unsafe decisions, background stability and physical cost for both evaluators.
