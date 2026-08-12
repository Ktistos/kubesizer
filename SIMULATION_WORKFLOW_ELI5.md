# Simulation Calibration and Optimization — Explain Like I’m 5

## The big idea

Imagine a city made from little buildings and roads.

- A **service** is a building.
- A **request** is a little car delivering a package.
- A **route** tells the car which building to visit next.
- A **replica** is an open worker window at a building.
- A **queue** is a line of cars waiting outside.
- An **SLO** is a promise such as: “95 out of 100 cars should finish their trip in less than one second.”

The simulator helps us decide how many worker windows each building needs.

But there is a problem: every simulation begins with an empty city. There are no cars and no queues. A real city that has been running all day is usually not empty.

So we first let the simulated city wake up before trusting its measurements.

---

## The complete workflow

```text
Create the simulated city
            ↓
Load or create a calibration profile
            ↓
Find the warm-up time W
            ↓
Find the measurement time M
            ↓
Try different replica configurations
            ↓
Choose a statistically feasible configuration
            ↓
Test the chosen configuration again with fresh randomness
            ↓
Only then continue to node placement
```

There are three separate stages:

1. **Calibration**: create the rules for trustworthy simulation experiments.
2. **Optimization evaluation**: use those frozen rules while trying replica configurations.
3. **Final validation**: test the winner again using completely fresh simulations.

Information and random seeds are not mixed between these stages.

---

# Stage A — Calibration

## 1. Find the smallest stable number of replicas

The analytical model first calculates how much traffic reaches every service.

This includes:

- cars entering from outside;
- cars routed from other services;
- cars revisiting a service;
- cars travelling around routing loops.

For every service, it finds the smallest replica count where the service can process traffic faster than traffic arrives.

In mathematical form, a service must satisfy:

```text
arrival rate < replica count × service rate
```

The resulting configuration is called the **minimum stable configuration**.

It is used as the main calibration configuration because it has the least spare capacity. It is usually the place where queues grow most easily and the system takes longest to settle.

A configuration that is analytically unstable is never used for steady-state calibration.

---

## 2. Find the warm-up duration W

### Why do we need W?

At time zero, the city is empty:

```text
cars = 0
queues = 0
busy workers = 0
```

This empty beginning can make latency look better than it really is.

The warm-up duration `W` answers:

> How long should we let the city run before it looks like a normally operating city rather than a newly opened city?

### What does the simulator watch?

During warm-up pilots, it records:

- the number of active end-to-end requests;
- the queue length at each service;
- the number of busy replicas at each service;
- the number of requests at each service;
- service utilization.

The simulator records state whenever an event changes it. The calibration code then calculates exact time-weighted bucket averages. It does not simply look at snapshots at bucket boundaries, because a snapshot could miss a traffic jam that happened between two snapshots.

### How is W selected?

The calibration code:

1. Runs many independent empty-start simulations.
2. Averages matching time buckets across the simulations.
3. Uses the final 20% of the pilot as a late-run reference.
4. Looks for three consecutive windows close to that reference.
5. Checks that those windows do not still have a clear upward or downward trend.
6. Finds a cutoff separately for every important state metric.
7. Uses the latest metric cutoff.
8. Adds one safety window.

It also checks different bucket widths and longer pilot horizons. If `W` changes too much when the horizon is doubled, calibration continues with a longer pilot or fails.

If an important metric never settles, the program does not invent a warm-up duration. Calibration fails explicitly.

---

## Is warm-up calibration based on topology?

**Yes, but not only topology.**

Topology means the services and the roads between them.

For example, this network may settle quickly:

```text
api → database → exit
```

This network may settle more slowly:

```text
api → worker → database → worker → database → exit
```

The second network has repeated visits and a routing loop, so traffic can stay inside it longer.

Warm-up also depends on:

- external arrival rates;
- routing probabilities;
- service rates;
- request classes;
- bottlenecks;
- replica bounds;
- simulator behaviour.

Therefore, changing the topology, routing, traffic, service speed, or simulator semantics invalidates the old calibration profile.

### Is W recalibrated for every replica configuration?

No.

The program calibrates at the minimum stable configuration and then validates the proposed `W` on a representative set containing:

- the minimum stable configuration;
- intermediate configurations;
- the bounded maximum configuration;
- extra near-SLO configurations when they are known.

It then freezes one conservative global value:

```text
W_global = the largest required validated warm-up
```

Every optimizer configuration inside the calibrated bounds uses the same `W_global`.

---

## 3. Find the measurement duration M

Warm-up time and measurement time do different jobs:

- `W` reduces empty-start bias.
- `M` provides enough latency data to estimate p95 precisely.

The initial measurement duration is based on the slowest required request-class arrival rate:

```text
M0 = safety factor × target samples / slowest class arrival rate
```

The program studies durations such as:

```text
M0
2 × M0
4 × M0
```

For every required request class, a candidate duration must have:

- enough eligible requests;
- enough independent nonempty replications;
- a sufficiently precise p95 confidence interval;
- a p95 that is stable after the duration is doubled;
- a confidence interval whose change after doubling is practically small.

The study is performed at representative replica configurations. If different configurations need different durations, the largest accepted duration becomes `M_global`.

Rare request classes are not ignored. If they do not have enough observations, measurement calibration fails.

---

## 4. Freeze the experiment rules

Successful calibration creates an immutable calibration profile containing:

- `W_global`;
- `M_global`;
- warm-up bucket and window settings;
- minimum and maximum optimization replications;
- replication batch size;
- maximum sequential looks;
- overall statistical error rate;
- p95 confidence-interval method;
- minimum sample requirements;
- disjoint random-seed namespaces;
- topology, workload, SLO, bounds, and simulator fingerprints;
- calibration diagnostics.

The default optimization replication policy is:

```text
R_min = 30
R_max = 60
batch size = 10
maximum looks = 4
```

These are configurable experiment settings, not universal magic numbers.

---

# Stage B — Optimization Evaluation

## 5. Start from the minimum stable configuration

The replica optimizer starts at the analytical minimum stable configuration and only adds replicas.

Before simulating a candidate, it checks analytical utilization.

If any service receives at least as much work as its replicas can process, the result is:

```text
ANALYTICALLY_UNSTABLE
```

The simulator is skipped for that candidate.

---

## 6. Run one simulation replication

Every independent replication follows this timeline:

```text
0 ---------------- W ---------------- W + M ---------------- completion
       warm-up          measured arrival window       tagged requests drain
```

Nothing resets at `W`.

The following continue normally:

- queues;
- busy resources;
- requests already in flight;
- routing;
- simulation time;
- random-number streams.

### Which requests are measured?

A request arriving before `W`:

- stays in the network;
- affects queues and other requests;
- is excluded from latency measurement.

A request arriving during `[W, W + M)`:

- is tagged for measurement;
- is included even if it finishes after `W + M`.

A request arriving at or after `W + M`:

- continues as background traffic;
- is not part of the measurement cohort.

After `W + M`, the simulator continues until every tagged request finishes. This prevents slow requests from being cut off and disappearing from p95.

---

## 7. Calculate p95 and its confidence interval

For each request class, latency observations remain grouped by simulation replication.

The p95 point estimate is calculated from all pooled eligible latencies. It is not the average of the individual replication p95 values.

Uncertainty is calculated with a **replication-cluster bootstrap**:

1. Select complete simulation replications with replacement.
2. Include every latency from each selected replication.
3. Pool those latency clusters.
4. Calculate p95.
5. Repeat this thousands of times.
6. Use the bootstrap p95 distribution for the lower and upper bounds.

Selecting complete replications preserves relationships between requests that experienced the same simulated run.

Bootstrap randomness uses a separate seed namespace and cannot change simulation randomness.

---

## 8. Decide whether a configuration passes

At each permitted sequential look, every request class receives a p95 confidence interval.

### Statistically feasible

```text
every request-class upper bound <= its SLO
```

This is the only classification accepted as an optimizer goal.

### Statistically infeasible

```text
at least one request-class lower bound > its SLO
```

### Continue sampling

The interval overlaps the SLO and fewer than `R_max` replications have been used. Another fixed batch is added.

### Inconclusive

The interval still overlaps the SLO at `R_max`.

Inconclusive does not mean proven infeasible, but it is never silently accepted as feasible. The optimizer may continue to a larger replica configuration.

### How configurations are searched

The optimizer begins with the minimum analytically stable replica configuration
and uses uniform-cost search. Think of configurations as shelves ordered by how
many replicas were added:

```text
shelf 0: add no replicas
shelf 1: add one replica
shelf 2: add two replicas
...
```

It finishes cheaper shelves before considering more expensive ones. Analytical
utilization only decides which box to open first on the same shelf. It does not
pretend to predict how many replicas remain before an SLO will pass.

If every cheaper box is definitively infeasible, the first feasible shelf proves
the minimum replica cost. If a cheaper box remains `INCONCLUSIVE`, the larger
candidate may be validated as feasible, but the report says that minimality is
not proven and lists the unresolved cheaper configurations.

### Failure

Simulation or calibration failures stop calibrated optimization explicitly. They are not disguised as ordinary infeasibility.

---

## 9. Protect against checking too many times

Looking repeatedly at ordinary 95% intervals would increase the chance of making a false claim.

The workflow uses a conservative Bonferroni allocation across:

- two confidence-interval tails;
- all permitted sequential looks;
- all request classes.

```text
alpha_tail = overall_alpha / (2 × maximum looks × request classes)
```

This keeps the complete sequential decision process within the configured overall error budget.

---

# Stage C — Final Validation

## 10. Test the winner again

Optimization examines many configurations and can therefore favour a configuration that looked lucky on the optimization seeds.

To protect against this selection bias, the selected configuration is locked and tested again.

Final validation:

- uses the same frozen `W` and `M`;
- uses completely fresh simulation seeds;
- never reuses optimization latency observations;
- uses a fixed replication count;
- does not stop early;
- recalculates pooled p95 confidence intervals.

The final result passes only when:

```text
every fresh request-class upper bound <= its SLO
```

If final validation fails, the program does not report the configuration as SLO-feasible, and node placement does not start.

---

# What happens when main.py runs?

`main.py` performs the following steps:

1. Reads `workload.yaml` and `resources.yaml`.
2. Builds the queueing network.
3. Looks for `simulation_calibration_output/calibration_profile.yaml`.
4. If the profile is missing, runs complete calibration.
5. If the profile exists, verifies its fingerprints.
6. Rejects an incompatible profile instead of silently using it.
7. Runs calibrated replica optimization.
8. Runs fresh final validation.
9. Runs node placement only after final validation passes.

Calibration reports are written under:

```text
simulation_calibration_output/
```

They can include:

- the immutable YAML profile;
- machine-readable JSON;
- a human-readable Markdown report;
- warm-up bucket data;
- Welch state plots;
- metric-specific warm-up cutoffs;
- p95 versus measurement duration;
- confidence-interval width versus measurement duration;
- seed and sensitivity diagnostics;
- explicit failure reports when calibration refuses a result.

---

# When can the profile be reused?

The profile can be reused while all of these remain unchanged:

```text
same services
same routing and routing probabilities
same external arrival rates
same service rates
same request classes
same SLO definitions
same simulator semantics
same replica bounds
```

Changing only the current replica counts does not invalidate the profile when the new counts remain inside the calibrated bounds.

The profile must be rejected and recalibrated after an important topology, workload, SLO, bound, or simulator change.

---

# The simplest possible summary

Imagine measuring how quickly children finish a playground obstacle course.

1. First, let the playground fill up normally. That is `W`.
2. Then watch children enter for a long enough period. That is `M`.
3. Repeat the experiment independently many times. Those are the replications.
4. Accept a playground layout only when the uncertainty says it safely meets the time promise.
5. Test the chosen layout one last time with completely new children.

That is the full calibration, optimization, and validation workflow.
