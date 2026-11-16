# Proposed stateful evaluation protocol

Prepared 2026-11-16. These are planned experiments, not completed results.
No current comparison campaign, frozen input or target data is amended by
this branch.

## 1. Kernel semantics before application claims

Use deterministic demands and independent implementations of expected event
times to verify sequential, parallel, exclusive-choice, all-response,
k-of-n and background rules. Verify ongoing capacity use after early response,
late return ownership, deadlines, mixed request classes, fixed membership,
and fingerprints. These automated software tests do not establish predictive
accuracy against real databases or storage systems.

## 2. Freeze the experiment design before target measurements

Start with a controllable primary/follower service and a real separately
provisioned PostgreSQL configuration. Evaluate MinIO only after documenting
the installed version, server/drive/pool topology and actual read/write paths.
Do not change a StatefulSet replica count to create independent copies of one
standalone data directory.

For each supported resolved topology, declare ahead of time:

- operation classes (reads, writes, payload-size bins) and their arrival mix;
- characterization windows/seeds, target windows/seeds and repetition counts;
- call/acknowledgement semantics and exactly which resources they occupy;
- per-member resource budgets and the independent evidence source for demand;
- application configuration, dataset, key-popularity distribution and cache state;
- latency boundary, SLOs, error/timeout criteria and stationarity checks;
- valid candidate membership/layouts and their physical deployment costs;
- load points, minimum sample counts, confidence procedure and abstention rules.

Freeze model inputs from characterization only. Never estimate replication
work or tune quorum/phase assumptions from target latency. Parameter selection
and pilot remediation must precede scored targets and be recorded.

## 3. Factors

Include low, medium and near-capacity loads, e.g. declared reference fractions
0.2, 0.5, 0.7, 0.85, 0.95 and one overload point. These are design labels until
actual CPU, disk, network and worker occupancy are independently measured.
Vary the read/write mix, object/payload size and balanced versus skewed
routing. Include Poisson-like and bursty arrivals. Compare local completion,
asynchronous propagation, all-response and k-of-n acknowledgement only where
the tested product actually supports the corresponding configuration.

Measure follower backlog/lag and device saturation as well as client latency.
A write that acknowledges quickly while replication falls behind is not a
healthy feasible decision. Keep failures and incomplete outcomes in scoring.
Repair/rebalance/failure workloads constitute a separate track until their
mechanisms have been modeled; do not mix them into healthy steady-state claims.

## 4. Comparators and ablations

Freeze the existing interchangeable-replica baseline and this branch before
scoring. Compare equal information conditions and report any additional
application-specific telemetry. Useful ablations are role-aware routing,
replication work, early acknowledgement semantics, and explicit storage
queues. Retain the measured latency boundary residual consistently; do not
re-fit it on targets. Do not force unsupported analytical models to emit an
apparently comparable finite prediction.

## 5. Report

Report per topology and per load: mean absolute percentage error and median
absolute percentage error, signed error, tails of the error distribution,
coverage, abstentions, failures, safe/unsafe recommendations and resource
cost. Use independent complete load-test runs for empirical uncertainty;
Monte Carlo confidence does not cover model error. Show follower/storage
stability alongside client latency. Keep conditional approximations distinct
from product-level evidence and test a configuration change beyond the
characterization topology before claiming stateful sizing generalization.

## Current status

Only local semantic verification is authorized and performed during this
implementation. Live stateful experiments have not been launched. Existing
comparison campaign services and artifacts are left untouched.
