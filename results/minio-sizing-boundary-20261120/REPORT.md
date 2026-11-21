# MinIO sizing near the PUT latency SLO

Four members met both mean-latency SLOs at 28 and 30 total requests/s. At 32 requests/s, the four-member PUT interval crossed its 400 ms SLO. Kubesizer recommended eight members at all three rates, and the physical feasibility of those recommendations remained inconclusive. At two rates there was already a cheaper measured feasible option. [Scoring results](data/scores.json) · [Frozen recommendations](data/predictions-frozen.json)

## Experimental design

The candidates were the same four- and eight-member EC:2 layouts as in the first sizing pilot. The two-set layout shared the same four workers and Longhorn backend used by the one-set layout. The workload used equal independent GET and PUT arrival streams and the same 1 MiB object data. The SLOs were 250 ms mean client latency for GET and 400 ms for PUT. The cost was the number of active members. [First sizing pilot](../minio-sizing-mis1-20261119/REPORT.md) · [Verified four-member layout](data/layout-4-verified.json) · [Verified eight-member layout](data/layout-8-verified.json)

In the exploratory sweep, the four-member PUT mean was 370.0 ms at 28 requests/s and 455.8 ms at 32 requests/s. The predeclared rule selected those two rates and their midpoint for holdout. Kubesizer reused the earlier aggregate characterization without updating its fitted demands or latency residuals from the sweep. The aggregate model's decisions were frozen before the holdouts. [Protocol before launch](protocol-before-launch.json) · [Sweep observations](data/exploration.json) · [Input provenance](data/prior-evidence.json)

Each load used four fresh pairs. The two layouts in a pair had identical scheduled times, operations and keys. Each window had a 15-second warm-up and a 75-second measurement period. GET and PUT streams were generated independently at equal nominal rates. The feasibility rule used two-sided 95% Student t intervals over the four physical run means for each operation, plus client scheduling, request failure and drain checks. [Target plan](data/target-plan.json) · [Run summaries](data/target-results.json)

## Observed sizing decisions

| Total requests/s | Four-member PUT mean and interval | Four-member outcome | Eight-member outcome | Frozen choice |
| ---: | --- | --- | --- | --- |
| 28 | 321.8 ms (311.2–332.4) | Passed both SLOs | Inconclusive. PUT mean 432.3 ms, interval 392.1–472.4 ms | 8 |
| 30 | 374.3 ms (350.1–398.6) | Passed both SLOs | Inconclusive. Client reached its in-flight limit in all four windows | 8 |
| 32 | 408.2 ms (388.1–428.3) | Inconclusive | Inconclusive. Client reached its in-flight limit in three windows | 8 |

The four-member GET mean was 107.2 ms or less across the tested rates. At 28 requests/s both layouts passed the client validity checks, and all four paired comparisons gave higher PUT means on the eight-member layout. The average paired increase was 110.5 ms, with an exploratory paired 95% interval of 78.7–142.3 ms. The eight-member PUT interval still crossed the 400 ms SLO, leaving its absolute feasibility inconclusive. [Scoring results](data/scores.json) · [Paired run means](data/target-results.json)

Seven eight-member windows at 30 or 32 requests/s reached the client's 48-request in-flight limit. The measurement cohorts contained 162 starts rejected by that limit and one separate PUT protocol error among 54,144 scheduled requests. The cap changed the offered workload reaching the server, so averages from the affected windows cannot certify SLO feasibility at the requested load. The decision ledger contains zero confirmed feasible selections, zero confirmed infeasible selections and three inconclusive selections. [Raw request windows](data/windows) · [Evidence audit](verification.json)

## Implications for the model

The frozen aggregate model treated each erasure set as one serialized I/O queue and transferred the same estimated work per operation to both layouts. With one set, the modeled offered work per unit time was 0.96, 1.03 and 1.10 at the three target rates. The optimizer left the smaller layout unresolved at 28 requests/s and rejected it at the modeled necessary stability floor at 30 and 32 requests/s. The measured four-member layout passing at 30 requests/s shows that the model's one-set stability floor excluded a layout that met both SLOs in this finite trial. These windows cannot establish steady-state stability. [Aggregate inputs](data/profile-aggregate.json) · [Frozen model diagnostics](data/predictions-frozen.json)

The eight-member result at 28 requests/s shows higher PUT latency with the extra set on these shared workers. Host contention, Longhorn activity and changes in request paths remain possible explanations. The experiment established the highest tested passing rate of 30 requests/s for four members and left the next point unresolved. A rate at which the larger layout is necessary and meets the SLOs remains unestablished. [Verified topologies](data/layout-8-verified.json) · [Scoring results](data/scores.json)

## Evidence and cleanup

The audit checked the archived source and simulator hashes, the original characterization, all 12 matched holdout schedules, and the order of the prediction freeze and physical runs. Offline scoring reproduced the recorded results byte for byte. The test pods were stopped and the stock entrypoints restored. The separate simulator comparison remained paused. [Verification](verification.json) · [Execution archive](execution-source.tar.gz) · [Cleanup](data/cleanup.json) · [Restoration](data/template-restoration.json)
