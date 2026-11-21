# Component examples

These inputs are synthetic and use the normal `kubesizer --workload FILE`
interface. They do not contain measured deployment performance claims.

| File | Demonstration |
|---|---|
| `evidence-worker.yaml` | Typed portable CSV CPU-demand estimate, bounded queue, failure budget |
| `broker-workers.yaml` | Background publication, processing vs prefetched slots, all-work job latency |
| `network-pool.yaml` | Gateway/connection holding, shared link bytes and disjoint one-way transit |
| `external-api.yaml` | Fixed quota, token wait, failed attempts, timeout/backoff retries |
| `cache.yaml` | Key-based routing, shared LRU/TTL state, conditional backend work |
| `serverless.yaml` | Fixed instance candidates with warm/cold/idle lifecycle |

The external API example intentionally retains the original tight failure
budget: the software integration run could not certify it and returned no
recommendation. A runnable model need not produce a feasible plan.

See [the component guide](../../docs/PORTABLE_COMPONENTS.md) for field definitions,
measurement adapters and analytical coverage. Independent delay and compatible
static resource stages have analytical forms; dynamic policies require simulation.
These examples do not model a full broker, cache product, TCP stack or serverless
autoscaling controller. GPU modeling is excluded.
