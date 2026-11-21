# MinIO sizing across erasure-set counts

The trial compares two independent MinIO object stores in the pilot namespace. The four-member layout uses one four-drive erasure set. The eight-member layout uses two four-drive sets in the same pool. The pinned MinIO build supports an explicit set width, and an object is stored within one set. Both layouts declare EC:2 and require live verification of the set width, parity, online drives and the worker placement of every drive before measurement. Each set spans the same four worker hosts with one drive per host. The cost is the number of active MinIO members and their dedicated Longhorn volumes. [Pinned MinIO layout code](https://github.com/minio/minio/blob/RELEASE.2025-10-15T17-29-55Z/cmd/endpoint-ellipses.go) · [Pinned design guide](https://github.com/minio/minio/blob/RELEASE.2025-10-15T17-29-55Z/docs/distributed/DESIGN.md)

Both systems use fresh 2 GiB per-member claims and separate buckets with a seeded 1 MiB payload and the same GET and PUT key sets. The client is placed on the same worker in each layout. A request's key suffix selects the entry member modulo the member count. This balances the declared entry routing, while MinIO chooses the actual erasure set. Every GET checks the received bytes. The storage headroom gate reserves enough free space for that window's entire planned writes on a single set plus metadata allowance.

## Characterization and model assumptions

The smaller layout supplies an idle window and two single-class 3 requests/s windows for each operation. The aggregate arm uses incremental cgroup CPU, the volume block busy-time counter and the low-load client mean for each class. Network counters describe transfer volume. They do not identify network queue occupancy or provide an independent network service demand for this model.

The model uses one CPU-sharing replica per physical member and one effective I/O queue per erasure set. It represents total per-request CPU work as a visit to one representative member. The stripe queue uses mean busy time per drive as an effective service proxy. A fixed residual is calculated from the low-load client mean after accounting for the explicit modeled work. It includes remaining transport and server phases that the fitted queues do not distinguish. These are modeling assumptions tested by the holdout.

The fragment arm uses the same CPU model and replaces the PUT I/O demand with the distribution of the largest receiver write-and-flush time in each matched four-fragment group. That duration is an observed receiver-path proxy. The model serializes it after CPU, and the baseline latency fit accounts for the remaining response time. The receiver measurements do not uniquely identify physical drive occupancy under shared storage or the overlap between phases. The phase fit requires complete fragment groups for at least 80% of tagged PUTs in each characterization window.

Both arms transfer the smaller layout's operation demands to the larger layout under equal-work and balanced-set assumptions. At each tested rate, the full simulation CLI compares the declared layouts. An unsupported decomposition, inadequate characterization or a failing model gate produces abstention.

## Holdout and decision scoring

The target has equal, independent GET and PUT arrival streams at 4, 8 or 12 requests/s per class. The mean client-latency SLO is 250 ms for GET and 400 ms for PUT. Each target run has a 12-second warm-up and a 45-second measurement window. Each rate has three physical pairs. The paired layouts use the same externally generated schedules and keys. Characterization evidence, candidate models, recommendations and target order are bound before the first target.

A physical layout passes when both class 95% Student t upper bounds are within their SLOs, the client met its scheduling gate, failures stayed within 1% for each class, and the request cohort drained within 30 seconds. A lower interval above the SLO or an excessive failure rate makes a sufficiently observed layout infeasible. Other cases are inconclusive. The pilot reports selections that passed, infeasible acceptances, cheaper measured feasible alternatives and abstentions. Three short runs at a rate provide exploratory evidence.

The new resources are isolated in the existing experimental namespace. Both layouts remain online during the paired targets, and only one receives the trial's S3 traffic at a time. Their common worker nodes and Longhorn backend can carry background work. Cleanup scales the twelve pilot members to zero and restores their stock startup templates. Persistent claims and raw data are retained. The original MinIO pilot and the comparison campaign stay idle.

## Follow-up near the mean SLO

The completed sizing pilot had a measured feasible four-member layout at every tested rate through 24 total requests/s. The follow-up reuses its aggregate characterization, the two layouts, the MinIO binary and the 1 MiB dataset. It keeps the equal GET/PUT mix and the 250 ms GET and 400 ms PUT mean SLOs. [First sizing pilot](../results/minio-sizing-mis1-20261119/REPORT.md)

The four-member layout gets an exploratory sweep at 28, 32, 36 and 40 total requests/s. The sweep stops at the first apparent SLO miss or invalid client window. The last apparent passing rate, the first apparent failing rate and their midpoint define the target rates. The previous pilot supplies the starting passing rate of 24 requests/s. If the sweep finds no valid miss, the two highest valid rates and their midpoint are used. The exploratory runs select rates and do not enter the model fit or the holdout score.

The aggregate model's decisions and the new target order are frozen before the first holdout. Four fresh schedule-matched pairs at each rate use a 15-second warm-up and a 75-second measurement window. Scoring uses the same failure and client checks and 95% Student t intervals across the four run means. The lower and upper confidence bounds determine whether a layout missed or met the latency SLO. These finite, controlled windows describe sizing on the four shared workers and their existing Longhorn storage. [Boundary runner](../scripts/run_minio_sizing_boundary.py)

## Confirmation with a larger client

The boundary study reached its client in-flight limit in seven eight-member windows at 30 and 32 total requests/s. The confirmation reuses the original frozen forecasts byte for byte and tests those rates with fresh seeds. Both layouts retain the same placement, storage, dataset and SLOs. Each observer client permits up to 128 in-flight requests, with limits of one CPU and 384 MiB. The run starts with one delivery check per layout at 32 requests/s before collecting four fresh pairs per rate. It checks the captured schedules against the generated workloads and records how many requests started and the peak concurrent SDK calls in each window. [Boundary study](../results/minio-sizing-boundary-20261120/REPORT.md)
