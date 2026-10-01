"""Machine-readable contract for reproducible operational measurements."""

from __future__ import annotations

from .models import (
    DEFAULT_MAXIMUM_CAPACITY_UTILIZATION,
    MEASUREMENT_METHODOLOGY_VERSION,
)


def measurement_methodology_contract():
    """Return the normalized evidence semantics expected by Kubesizer.

    Cluster collectors may discover different concrete label names, but they
    must map them to these identities and quantities.  Keeping this contract in
    code lets an exporter validate compatibility before producing a manifest.
    """
    return {
        "version": MEASUREMENT_METHODOLOGY_VERSION,
        "windowing": {
            "operation": "counter_increase",
            "alignment": "common_non_overlapping_fixed_duration_windows",
            "clock": "utc",
            "load_phase": "post_warmup_fixed_replica_observation",
        },
        "service_requests": {
            "metric": "istio_requests_total",
            "reporter": "destination",
            "quantity": "completed_requests",
            "attribution": ["namespace", "destination_pod"],
        },
        "optional_request_class_service_counts": {
            "metric": "istio_requests_total",
            "reporter": "destination",
            "attribution": ["service", "request_operation"],
            "normalized_source": "service_class_intervals",
            "cpu_attribution": "not_inferred_per_request_class",
        },
        "optional_external_requests": {
            "metric": "istio_requests_total",
            "reporter": "destination",
            "attribution": "known_entry_workloads",
            "unknown_source_semantics": (
                "external_only_after_restricting_destination_to_known_entries"
            ),
            "request_class_label": "request_operation",
        },
        "external_arrival_process": {
            "source": (
                "complete request-start timestamps from the declared external "
                "boundary, normally an Istio/Envoy gateway access log or trace"
            ),
            "required_for": "latency_slo_recommendation",
            "attribution": ["request_class", "entry_service_when_multi_entry"],
            "timestamp_order": (
                "nondecreasing; equal timestamps preserve observed request batches"
            ),
            "minimum_interarrival_gaps_per_stream": 30,
            "simulation": (
                "mean-rate-rescaled moving trace-block bootstrap preserving "
                "local arrival ordering"
            ),
            "fallback": (
                "exponential only for workloads without a declared latency SLO"
            ),
            "compression": "lossless gzip is accepted; time aggregation is not",
            "forecast_boundary": (
                "certification is conditional on the measured burst family and "
                "declared load envelope"
            ),
        },
        "optional_service_edges": {
            "metric": "istio_requests_total",
            "reporter": "source",
            "quantity": "completed_calls",
            "attribution": ["source_service", "destination_service"],
        },
        "optional_latency": {
            "metric": "istio_request_duration_milliseconds_bucket",
            "aggregation": "sum_histogram_buckets_before_quantiles",
            "unit_after_normalization": "seconds",
            "bucket_grid": "identical_per_subject_across_observed_windows",
            "terminal_bucket": "positive_infinity_required",
            "count_consistency": (
                "terminal cumulative count matches completed requests within "
                "one_percent_or_one_request"
            ),
        },
        "optional_latency_boundary_components": {
            "external_client": {
                "client_to_ingress": (
                    "load-generator latency minus matched ingress request latency"
                ),
                "ingress_processing": (
                    "ingress request duration minus upstream response duration"
                ),
            },
            "ingress": {
                "ingress_processing": (
                    "ingress request duration minus upstream response duration"
                ),
            },
            "quantile_rule": (
                "derive component samples or distributions; never subtract "
                "independently aggregated percentiles"
            ),
            "automatic_client_to_entry_mean": {
                "formula": (
                    "aligned external-client response mean minus aligned "
                    "application-entry response mean"
                ),
                "minimum_common_windows": 3,
                "count_alignment": (
                    "within five_percent_or_one_request in at least "
                    "eighty_percent of candidate windows"
                ),
                "health_gate": "at_most_one_percent_errors",
                "stability_gate": (
                    "window residual coefficient_of_variation at most 0.20 "
                    "and relative_range at most 0.50"
                ),
                "scope": "external_client_boundary_with_mean_slo_only",
                "explicit_evidence_precedence": True,
                "failure_semantics": (
                    "analytical and simulation latency evaluation reject an "
                    "unidentifiable required boundary"
                ),
            },
            "unmodeled_residual": (
                "allowed only as explicitly named residual evidence, never folded "
                "into service demand"
            ),
        },
        "optional_service_network_latency": {
            "measurement": (
                "HTTP health probe round trips between application pods with "
                "sidecars on the same node and on different nodes"
            ),
            "normalization": "round_trip_seconds / 2",
            "outputs": ["same_node_one_way", "cross_node_one_way"],
            "cross_node_probability": (
                "observed or estimated fraction of service calls crossing nodes"
            ),
        },
        "optional_service_non_cpu_blocking_time": {
            "purpose": (
                "represent I/O, lock, or external wait that releases CPU while "
                "retaining an admitted worker/concurrency slot"
            ),
            "source": (
                "low-load aligned Istio service response mean minus CPU "
                "execution mean, modeled downstream response, and measured "
                "communication round trips"
            ),
            "cohort_alignment": (
                "response mean and CPU execution mean use the same "
                "response-bearing service interval rows"
            ),
            "low_load_gate": "maximum observed CPU utilization at most 0.20",
            "output": "traffic_model.service_times.<service>.non_cpu_blocking_time",
            "uncertainty": (
                "the counter-derived result is an inferred mean residual; for "
                "eligible leaf services, mean and p50 parameterize a capped "
                "lognormal assumption but do not prove that distribution family"
            ),
            "double_counting_rule": (
                "exclude downstream and communication latency already modeled"
            ),
            "parallel_calls": (
                "aggregate child means cannot identify parent wait for parallel "
                "calls; explicit blocking evidence is required"
            ),
            "failure_semantics": (
                "production simulation rejects missing, high-load, or "
                "incompatible blocking evidence instead of assuming zero"
            ),
        },
        "optional_errors": {
            "metric": "istio_requests_total",
            "groups": ["4xx", "5xx"],
            "denominator": "all_completed_requests_same_scope_and_window",
        },
        "optional_capacity_envelope": {
            "sample_unit": "independent_fixed_replica_load_test_run",
            "identity": ["run_id", "service", "replicas"],
            "required_quantities": [
                "offered_rate_rps",
                "completed_rate_rps",
                "error_rate",
                "healthy",
            ],
            "healthy_semantics": (
                "the run tracked offered throughput with bounded backlog and met "
                "predeclared error, scheduler-delay, and stationarity gates; any "
                "latency safety ceiling is distinct from the optimizer's target SLO"
            ),
            "statistical_bound": (
                "one-sided Student-t lower confidence bound over complete runs"
            ),
            "replica_scaling": (
                "bounded by observed replica count; no automatic linear "
                "per-replica extrapolation"
            ),
        },
        "statistically_controlled_stability_floor": {
            "arrival_bound": (
                "one-sided Student-t upper confidence bound over complete "
                "aligned workload windows"
            ),
            "capacity_bound": (
                "one-sided Student-t lower confidence bound over independent "
                "healthy capacity runs, normalized per replica"
            ),
            "multiplicity": (
                "Bonferroni over every service-arrival and tested-capacity bound"
            ),
            "rule": "replicas * usable_capacity_lower_bound > arrival_upper_bound",
            "headroom": {
                "input": "workload.sizing_policy.maximum_capacity_utilization",
                "default": DEFAULT_MAXIMUM_CAPACITY_UTILIZATION,
                "meaning": (
                    "maximum confidence-bounded offered-load ratio against "
                    "measured healthy capacity"
                ),
            },
            "unobserved_replica_counts": (
                "explicitly labelled conditional extrapolation using the worst "
                "bounded per-replica rate seen at tested counts"
            ),
            "failure_semantics": (
                "missing evidence or a floor above the configured bounds is "
                "reported; analytical stability is not substituted as proof"
            ),
            "separation_from_slo": (
                "the floor screens capacity stability; request-class SLO "
                "feasibility remains a later analytical or simulation decision"
            ),
        },
        "application_cpu": {
            "metric": "container_cpu_usage_seconds_total",
            "quantity": "cpu_seconds_consumed",
            "attribution": ["namespace", "pod"],
            "excluded_containers": ["", "POD", "istio-proxy"],
        },
        "optional_cpu_throttling": {
            "metric": "container_cpu_cfs_throttled_seconds_total",
            "operation": "counter_increase",
            "containers": "same_application_container_set_as_cpu_usage",
        },
        "optional_memory": {
            "metric": "container_memory_working_set_bytes",
            "containers": "same_application_container_set_as_cpu_usage",
            "operation": "time_mean_or_max_over_window",
        },
        "replica_join": {
            "keys": ["namespace", "pod"],
            "cardinality": "one_normalized_row_per_pod_and_window",
        },
        "cpu_capacity": {
            "source": "kubernetes_container_cpu_requests",
            "quantity": "cores_reserved_per_application_replica",
            "cpu_limit_semantics": "reported_ceiling_not_guaranteed_capacity",
        },
        "derived_cpu_demand": {
            "formula": "sum(cpu_seconds) / sum(completed_requests)",
            "unit": "cpu_seconds_per_request",
            "sample_unit": "service_observation_window",
            "zero_throughput": "excluded",
            "per_request_distribution_identifiable": False,
        },
        "initial_sizing_evidence_gate": {
            "blocking_conditions": [
                "missing_request_class_arrival_rate",
                "missing_declared_slo_statistic",
                "missing_cpu_demand",
                "material_flow_balance_mismatch",
                "excessive_5xx_rate",
                "excessive_cpu_throttling",
                "load_dependent_extrapolation",
                "incomplete_simulation_blocking_characterization",
                "missing_request_class_arrival_trace_for_latency_slo",
                "missing_multi_entry_arrival_trace_attribution_for_latency_slo",
            ],
            "conditional_conditions": [
                "missing_request_class_arrival_trace_without_latency_slo",
                "unknown_multi_entry_trace_attribution_without_latency_slo",
                "inferred_call_order",
                "missing_communication_latency",
                "missing_concurrency_limit",
                "recommended_replica_count_not_load_tested",
            ],
            "probability_semantics": (
                "confidence bounds and measured ranges are reported; they are "
                "not converted to a probability of SLO compliance"
            ),
        },
        "closed_loop_quality_experiment": {
            "sample_unit": "independent_complete_load_test_run",
            "arrival_cohort": "actual_client_start_in_half_open_measurement_window",
            "post_window_completion": "included",
            "latency_interval": "replication_cluster_bootstrap",
            "right_censoring": {
                "representation": "latency_lower_bound_at_observation_stop",
                "feasibility": "never_certified_from_a_censored_cohort",
                "infeasibility": (
                    "may_be_certified_when_the_lower_bound_interval_exceeds_the_slo"
                ),
                "http_failure": "not_inferred_from_censoring",
            },
            "replica_vector_identity": (
                "explicit_service_order_checked_against_measured_replica_counts"
            ),
            "multiplicity": (
                "Bonferroni over two tails, tested configurations, and constraints"
            ),
            "minimum_claim": (
                "minimum only inside an explicitly declared finite configuration "
                "space after every cheaper state is conclusively infeasible"
            ),
            "cluster_state": "restore_original_configuration_in_finally",
        },
        "capability_rule": "unavailable_metrics_are_reported_not_imputed",
    }
