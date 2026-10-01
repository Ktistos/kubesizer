# Frozen retrospective STELAR comparison

1. Fit only the two rate-4 runs per operation from the retained
   2026-08-22-admin-valid operation-characterization dataset. Never use the
   historical multi-load CPU regression or target service latencies as inputs.
2. Pool application CPU rates / completed-request rates over matched low-load
   windows and replicas. Window demand variation is not per-request variance:
   CPU work is deterministic in this comparison.
3. Derive sequential local blocking means from low-load destination response
   means minus direct-child response means and CPU work. Negative/unseparable
   residuals are omitted and explicitly reported; no fitted tail is invented.
4. Freeze the request-weighted client-minus-entry mean residual from rate-4
   characterization. Both methods get exactly the same residual, CPU/blocking
   inputs, call counts, arrival moments and replica counts.
5. The source uses one-core-normalized CPU capacity, not known CPU requests.
   Pool limits ckan=2, stelarapi=4 are a historically declared worker-count
   sensitivity. Their application provenance is not proven by telemetry.
   Also report an unknown-pool sensitivity. Do not label either an unconditional
   portable deployment capacity certificate.
6. Evaluate every rate-32 and rate-56 run for both dataset_search and
   organizations, two repetitions each. Use offered scheduled rate, not reduced
   completed throughput, and measured replica counts. Freeze the arrival SCV
   from characterization; no target-arrival/outcome fitting.
7. Compare relaxed baseline diagnostic means with enriched means. Report the
   baseline's production incompatibility reasons separately. An unsupported
   diagnostic is not a production recommendation.
8. Mean MAPE covers only uncensored runs with <1% failures. All runs remain in
   the failure/SLO safety table; rejected high-load points must not disappear.
   Mean SLO is the original 0.5 seconds. Failure >=1% is empirically unsafe even
   when successful-response mean passes the SLO.
9. Commit implementation and this protocol before executing the scorer. Record
   that commit and source hashes. Do not tune after inspecting the result.

This is retrospective. Historical outcomes have been inspected in previous
analysis, and it is not a preregistered or blinded holdout. The STELAR operations
exercise sequential real application calls, not a real fork-join validation.
