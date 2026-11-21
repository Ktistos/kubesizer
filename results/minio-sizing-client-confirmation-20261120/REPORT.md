# MinIO sizing confirmation with a larger client

The confirmation completed all 16 holdout windows. Eight MinIO members had slower PUTs than four in every pair where both clients delivered the prescribed load. Four consecutive windows, covering both layouts, experienced a slowdown and hit the new client limit. The predeclared scoring therefore remained inconclusive at both tested rates. [Scores](data/scores.json) · [Evidence audit](verification.json)

## Design

The experiment reused the earlier frozen eight-member recommendations at 30 and 32 total requests/s. Both layouts ran on the same four workers with their existing Longhorn volumes. It kept the equal GET/PUT workload, 1 MiB objects and mean-latency SLOs of 250 ms for GET and 400 ms for PUT. The client limit rose from 48 to 128 simultaneous requests, and both observer containers used limits of one CPU and 384 MiB. Each layout started all 1,871 scheduled requests during its 32 requests/s delivery check. The scored holdout used four fresh schedule-matched pairs at each rate, with 15 seconds of warm-up and 75 seconds of measurement per run. [Protocol](protocol-before-launch.json) · [Forecast provenance](data/retained-forecasts.json) · [Client delivery checks](data/client-precheck-8.json)

## What happened

Twelve holdout windows passed the client validity checks and started every scheduled request. The higher limit allowed some valid windows to reach 91 concurrent requests. Four consecutive windows beginning at 01:23 UTC on 20 November reached 128 concurrent requests. Across those windows, the client rejected 1,934 starts at its concurrency limit and another 210 because the same PUT key was still in flight. The last affected window also recorded 92 read timeouts and took about 42 seconds to drain. The two windows after it passed the client validity checks. The recordings establish the timing and impact of the slowdown. Its cause remains undetermined. [Per-window delivery](data/client-delivery.json) · [Run summaries](data/target-results.json) · [Raw windows](data/windows)

| Total requests/s | Fully valid pairs | Four-member PUT run means (ms) | Eight-member PUT run means (ms) | Predeclared result for both layouts |
| ---: | ---: | ---: | ---: | --- |
| 30 | 3 of 4 | 363–422 | 614–819 | Inconclusive |
| 32 | 3 of 4 | 365–425 | 603–975 | Inconclusive |

In the six valid pairs, the larger layout's PUT mean was 217–550 ms higher. Its PUT mean exceeded 400 ms in each of those windows. The table describes windows where complete traffic was delivered. The predeclared decision uses all four repetitions and requires valid client delivery in each, so it leaves both eight-member recommendations unconfirmed and both layouts formally inconclusive. The valid pairs show no PUT latency benefit from adding an erasure set on these shared workers. [Matched comparisons](verification.json) · [Scoring results](data/scores.json)

## Shared capacity sensitivity

A separate model tested the assumption that both erasure sets share one equivalent storage queue. It retained the original demand estimates and changed only the larger layout's modeled storage copy count from two to one. Both variants were evaluated before the confirmation holdouts. With one queue, offered work exceeded its modeled capacity at both rates, so Kubesizer abstained at 30 and 32 requests/s. This shows how sensitive the recommendation is to the independence assumption. The exercise did not measure shared storage capacity, and the modeled queue remains an approximation. [30 requests/s model result](shared-storage-sensitivity/r15/forecast.json) · [32 requests/s model result](shared-storage-sensitivity/r16/forecast.json)

## Verification and cleanup

Offline scoring reproduced the archived decisions byte for byte. The audit verified both topology snapshots, the raw schedules for all eight pairs, the original forecast and source hashes, and the ordering of the forecasts and measurements. The test pods were stopped, the original container templates restored, and the separate simulator comparison remained paused. [Evidence audit](verification.json) · [Cleanup](data/cleanup.json) · [Template restoration](data/template-restoration.json) · [Executed source](execution-source.tar.gz)
