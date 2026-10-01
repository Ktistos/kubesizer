# Experimental layered semantics

This branch adds an opt-in evaluator. The baseline remains the default:

```sh
./scripts/kubesizer --workload workload.yaml --analytical-semantics experimental-layered --json
```

It uses the existing workload schema, not a new nonportable data source.
CPU means/variability, local blocking means/variability, synchronous calls,
execution mode, processing slots, admitted concurrency and replica dispatch
are consumed separately. Explicit portable blocking declarations are preserved;
the portable builder does not invent missing local I/O durations.

For each service, children are solved first in the acyclic union of call graphs.
Its holding demand is local CPU work plus local blocking plus child response
time, including communication. A parent retains admission capacity while its
children execute. That does not reserve the parent's CPU while blocked.

The CPU queue is solved separately. Extra admission waiting is approximated as
`max(0, W(holding demand) - W(CPU-only demand))` for the declared admission
pool. The subtraction prevents duplicate FCFS queue waiting in the CPU-only
limit. It is a decomposition heuristic, not an exact solution of CPU/admission
coupling. CPU queue residence inside a held pool is not included; near saturation
this can underestimate occupancy and must be validated independently.

Sequential child means are summed. Counts use a fixed floor/ceil approximation;
mean call counts alone do not identify their actual variance. Parallel branches
use an independent moment-matched lognormal maximum, computed by deterministic
CDF integration. Integer fan-out is required; fractional fork membership is
rejected. Deterministic parallel durations reduce to their maximum. Branch
dependence is not identified, and a sum-of-branches sensitivity option exists
in the Python adapter. Its branch mean range is not a confidence interval or
a rigorous bound on the complete coupled queueing system.

Independent random replica queues receive rate/N and a thinning approximation;
round-robin uses an SCV/N approximation. They are not treated as one pooled
queue. Processor sharing uses the Poisson M/G/1-PS mean; non-Poisson input uses
an explicit SCV-factor approximation. Arrival variability propagation remains
the baseline network approximation, not a fine-grained burst correlation model.

Unsupported cyclic graphs, unidentified fork membership, deadlines and
percentile SLOs fail closed. Exact replica search may traverse states which are
CPU-stable but holding-pool-unstable. Its proof concerns only this approximate
bounded model, not empirical safety or global deployment feasibility.

Related semantics: [Carleton's LQN manual](https://www.sce.carleton.ca/rads/lqns/userman22.pdf)
describes software tasks holding resources during synchronous calls. This
implementation is deliberately a smaller approximation, not an implementation
of the full LQNS solver. [Fork-join response-time research](https://www.doc.ic.ac.uk/~wjk/publications/lebrecht-knottenbelt-ukpew-2007.pdf)
illustrates why synchronization and dependence cannot simply be replaced by
the sum of every visited service's mean.
