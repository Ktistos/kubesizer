"""Machine-readable contract for reproducible operational measurements."""

from __future__ import annotations

from .models import MEASUREMENT_METHODOLOGY_VERSION


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
                "the run met its predeclared latency, error, scheduler-delay, "
                "and stationarity gates"
            ),
            "statistical_bound": (
                "one-sided Student-t lower confidence bound over complete runs"
            ),
            "replica_scaling": (
                "bounded by observed replica count; no automatic linear "
                "per-replica extrapolation"
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
        "capability_rule": "unavailable_metrics_are_reported_not_imputed",
    }
