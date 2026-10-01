# Analytical-only baseline migration

This integration preserves both the statistical-workflow branch history and the
previous master history. It deliberately takes its implementation from the
statistical-workflow branch, not from the older master feature set.

Removed: native simulation, simulation calibration/validation, simulation CLI
options and tests. Scalable optimization and node-placement optimization remain
absent. Kept: portable measurements, CPU-demand characterization, inferred or
explicit latency-boundary residuals, analytical distribution moments, mean
latency evaluation, and exact bounded replica search.

Historical simulation reports were moved under docs/history. The simulation
branch and its source worktree are preserved, including uncommitted changes.

Verification on 2026-10-01:

- 198 retained/new analytical and measurement tests passed.
- Default CLI and portable-measurement CLI recommendations succeeded.
- 120 retained topology configurations reproduced all 133 previous analytical
  mean predictions with zero absolute numerical difference.

The parity comparison is diagnostic, not a claim of production support for the
unsupported semantics in those frozen models. New semantics belong on a
separate branch, followed by an explicitly retrospective STELAR replay.
