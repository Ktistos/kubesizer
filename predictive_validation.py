"""Held-out validation of simulator predictions against cluster observations.

Simulation-output confidence intervals quantify Monte Carlo uncertainty.  They
do not establish that the simulator represents the deployed system.  This
module supplies that separate predictive-validity check and an immutable
certificate that recommendation workflows can require.
"""

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math

from calibration.profile import experiment_fingerprints
from metrics import (
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
    replication_cluster_bootstrap_rate,
)
from native_backend import SIMULATOR_SEMANTICS_VERSION, get_native_backend


class PredictiveValidationStatus(str, Enum):
    VALIDATED = "VALIDATED"
    REJECTED = "REJECTED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


@dataclass(frozen=True)
class PredictiveValidationCase:
    """One fresh cluster observation paired with its exact simulation input."""

    name: str
    system: object
    configuration: tuple[int, ...]
    observed_request_class_latencies: tuple[tuple[str, float], ...]
    statistic: str = "mean"
    observed_request_class_error_rates: tuple[tuple[str, float], ...] = ()

    @classmethod
    def from_mappings(
        cls,
        name,
        system,
        configuration,
        observed_request_class_latencies,
        *,
        statistic="mean",
        observed_request_class_error_rates=None,
    ):
        return cls(
            name=str(name),
            system=system,
            configuration=tuple(configuration),
            observed_request_class_latencies=tuple(
                sorted(
                    (str(key), float(value))
                    for key, value in observed_request_class_latencies.items()
                )
            ),
            statistic=str(statistic),
            observed_request_class_error_rates=tuple(
                sorted(
                    (str(key), float(value))
                    for key, value in (
                        observed_request_class_error_rates or {}
                    ).items()
                )
            ),
        )


@dataclass(frozen=True)
class PredictiveValidationOptions:
    warmup_duration: float
    measurement_duration: float
    replication_count: int = 10
    workers: int = 1
    simulation_seed_start: int = 8_000_000
    bootstrap_seed_start: int = 8_500_000
    bootstrap_resample_count: int = 1_000
    confidence_level: float = 0.95
    minimum_cases: int = 2
    maximum_relative_underprediction: float = 0.10
    maximum_relative_overprediction: float = 0.25
    absolute_error_tolerance_seconds: float = 0.02
    maximum_relative_ci_half_width: float = 0.10
    maximum_observed_error_rate: float = 0.01
    maximum_absolute_error_rate_difference: float = 0.02
    maximum_error_rate_ci_half_width: float = 0.02
    require_observed_error_rates: bool = True
    maximum_drain_time: float = 10_000.0


@dataclass(frozen=True)
class RequestClassPredictionDiagnostic:
    request_class: str
    statistic: str
    observed_seconds: float
    predicted_seconds: float
    lower_bound_seconds: float
    upper_bound_seconds: float
    absolute_error_seconds: float
    relative_error: float
    relative_ci_half_width: float
    passed: bool
    reason: str | None = None


@dataclass(frozen=True)
class RequestClassErrorRatePredictionDiagnostic:
    request_class: str
    observed_rate: float
    predicted_rate: float
    lower_bound: float
    upper_bound: float
    absolute_error: float
    ci_half_width: float
    passed: bool
    reason: str | None = None


@dataclass(frozen=True)
class PredictiveValidationCaseResult:
    name: str
    configuration: tuple[int, ...]
    workload_fingerprint: str
    seeds: tuple[int, ...]
    diagnostics: tuple[RequestClassPredictionDiagnostic, ...]
    error_rate_diagnostics: tuple[
        RequestClassErrorRatePredictionDiagnostic, ...
    ]
    observed_request_class_error_rates: tuple[tuple[str, float], ...]
    passed: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PredictiveValidationResult:
    status: PredictiveValidationStatus
    target_experiment_fingerprint: str
    model_structure_fingerprint: str
    simulator_semantics_version: str
    cases: tuple[PredictiveValidationCaseResult, ...]
    reasons: tuple[str, ...]

    @property
    def accepted(self):
        return self.status is PredictiveValidationStatus.VALIDATED

    @property
    def certificate_fingerprint(self):
        payload = {
            "status": self.status.value,
            "target": self.target_experiment_fingerprint,
            "structure": self.model_structure_fingerprint,
            "semantics": self.simulator_semantics_version,
            "cases": [
                {
                    "name": case.name,
                    "configuration": case.configuration,
                    "workload": case.workload_fingerprint,
                    "seeds": case.seeds,
                    "passed": case.passed,
                }
                for case in self.cases
            ],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def assert_valid_for(self, system):
        expected = _experiment_fingerprint(system)
        if expected != self.target_experiment_fingerprint:
            raise ValueError(
                "Predictive-validation evidence belongs to a different "
                "topology or workload."
            )
        if _model_structure_fingerprint(system) != self.model_structure_fingerprint:
            raise ValueError(
                "Predictive-validation evidence uses different simulator model "
                "semantics."
            )
        if self.simulator_semantics_version != SIMULATOR_SEMANTICS_VERSION:
            raise ValueError(
                "Predictive-validation evidence uses an obsolete simulator "
                "semantics version."
            )
        if not self.accepted:
            raise ValueError(
                "The simulator has not passed held-out predictive validation."
            )


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _experiment_fingerprint(system):
    topology, workload = experiment_fingerprints(system)
    return _digest(
        {
            "topology": topology,
            "workload": workload,
            "semantics": SIMULATOR_SEMANTICS_VERSION,
        }
    )


def _model_structure_fingerprint(system):
    """Fingerprint semantics shared by validation cases at different loads."""
    payload = get_native_backend().serialize_system(system)
    payload.pop("name", None)
    for service in payload["services"]:
        replicas = service.pop("replicas")
        # Different replica counts are valid holdouts, different per-replica
        # processing/admission limits are not. Absence still means unlimited.
        service["capacity"] //= replicas
        if "concurrency_capacity" in service:
            service["concurrency_capacity"] //= replicas
    for stream in payload["external_streams"]:
        distribution = stream["interarrival_distribution"]
        stream["interarrival_distribution"] = {
            "distribution": distribution["distribution"],
            "scv": distribution["scv"],
        }
    return _digest(payload)


def _validate_options(options):
    for label, value, allow_zero in (
        ("warmup_duration", options.warmup_duration, True),
        ("measurement_duration", options.measurement_duration, False),
        ("absolute_error_tolerance_seconds", options.absolute_error_tolerance_seconds, True),
        ("maximum_drain_time", options.maximum_drain_time, False),
    ):
        if not math.isfinite(float(value)) or (
            float(value) < 0.0 if allow_zero else float(value) <= 0.0
        ):
            raise ValueError(f"{label} has an invalid value.")
    for label, value in (
        ("replication_count", options.replication_count),
        ("workers", options.workers),
        ("bootstrap_resample_count", options.bootstrap_resample_count),
        ("minimum_cases", options.minimum_cases),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} must be a positive integer.")
    if options.replication_count < 2:
        raise ValueError("Predictive validation requires at least two replications.")
    if not 0.0 < options.confidence_level < 1.0:
        raise ValueError("confidence_level must lie strictly between zero and one.")
    for label, value in (
        ("maximum_relative_underprediction", options.maximum_relative_underprediction),
        ("maximum_relative_overprediction", options.maximum_relative_overprediction),
        ("maximum_relative_ci_half_width", options.maximum_relative_ci_half_width),
        ("maximum_observed_error_rate", options.maximum_observed_error_rate),
        (
            "maximum_absolute_error_rate_difference",
            options.maximum_absolute_error_rate_difference,
        ),
        (
            "maximum_error_rate_ci_half_width",
            options.maximum_error_rate_ci_half_width,
        ),
    ):
        if not math.isfinite(float(value)) or float(value) < 0.0:
            raise ValueError(f"{label} must be finite and non-negative.")


def _validate_case_declaration(case):
    if not case.name:
        raise ValueError("Every predictive-validation case needs a name.")
    if case.statistic not in {"mean", "p95"}:
        raise ValueError("Predictive-validation statistic must be mean or p95.")
    classes = tuple(case.system.get_request_classes())
    observed = dict(case.observed_request_class_latencies)
    if set(observed) != set(classes):
        raise ValueError(
            "Observed latency values must cover every request class exactly."
        )
    if any(not math.isfinite(value) or value <= 0.0 for value in observed.values()):
        raise ValueError("Observed latency values must be finite and positive.")
    errors = dict(case.observed_request_class_error_rates)
    if not set(errors) <= set(classes) or any(
        not math.isfinite(value) or not 0.0 <= value <= 1.0
        for value in errors.values()
    ):
        raise ValueError("Observed error rates are invalid.")
    configuration = tuple(case.configuration)
    if len(configuration) != len(case.system.get_services()) or any(
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
        for value in configuration
    ):
        raise ValueError("Validation configuration is invalid.")


def _case_seeds(options, case_index):
    start = options.simulation_seed_start + case_index * options.replication_count
    return tuple(range(start, start + options.replication_count))


def _prediction_interval(clusters, request_class, statistic, options, seed):
    tail = (1.0 - options.confidence_level) / 2.0
    method = (
        replication_cluster_bootstrap_mean
        if statistic == "mean"
        else replication_cluster_bootstrap_p95
    )
    return method(
        clusters,
        request_class=request_class,
        slo=1.0,
        resample_count=options.bootstrap_resample_count,
        seed=seed,
        lower_tail_probability=tail,
        upper_tail_probability=1.0 - tail,
        minimum_tail_draws=5,
    )


def validate_simulator_predictions(target_system, cases, *, options):
    """Validate latency predictions on fresh cluster observations.

    The cases may use different arrival rates, but must share service demands,
    workflows, concurrency limits, and simulator semantics with the target.
    At least one case must match the target workload fingerprint exactly.
    """
    if not isinstance(options, PredictiveValidationOptions):
        raise TypeError("options must be PredictiveValidationOptions.")
    _validate_options(options)
    cases = tuple(cases)
    target_fingerprint = _experiment_fingerprint(target_system)
    structure_fingerprint = _model_structure_fingerprint(target_system)
    if len(cases) < options.minimum_cases:
        return PredictiveValidationResult(
            status=PredictiveValidationStatus.INSUFFICIENT_EVIDENCE,
            target_experiment_fingerprint=target_fingerprint,
            model_structure_fingerprint=structure_fingerprint,
            simulator_semantics_version=SIMULATOR_SEMANTICS_VERSION,
            cases=(),
            reasons=(
                f"At least {options.minimum_cases} independent held-out cases are required.",
            ),
        )

    results = []
    global_reasons = []
    target_case_present = False
    for case_index, case in enumerate(cases):
        _validate_case_declaration(case)
        case_workload_fingerprint = _experiment_fingerprint(case.system)
        target_case_present |= case_workload_fingerprint == target_fingerprint
        reasons = []
        diagnostics = []
        error_rate_diagnostics = []
        if _model_structure_fingerprint(case.system) != structure_fingerprint:
            reasons.append(
                "Case service/workflow semantics differ from the target model."
            )
        errors = dict(case.observed_request_class_error_rates)
        if options.require_observed_error_rates and set(errors) != set(
            case.system.get_request_classes()
        ):
            reasons.append("Observed error rates do not cover every request class.")
        timeouts = case.system.get_request_class_end_to_end_timeouts()
        excessive_errors = {
            name: value
            for name, value in errors.items()
            if value > options.maximum_observed_error_rate
            and timeouts.get(name, 0.0) <= 0.0
        }
        if excessive_errors:
            reasons.append(
                "Observed failures exceed the simulator's validated successful-"
                "request domain: "
                + ", ".join(
                    f"{name}={value:.3%}"
                    for name, value in sorted(excessive_errors.items())
                )
            )

        seeds = _case_seeds(options, case_index)
        original_configuration = tuple(case.system.get_system_replicas())
        try:
            case.system.configure_system_replicas(case.configuration)
            replications = case.system.simulate_measurement_replications(
                warmup_duration=options.warmup_duration,
                measurement_duration=options.measurement_duration,
                seeds=seeds,
                workers=options.workers,
                maximum_drain_time=options.maximum_drain_time,
            )
            for class_index, request_class in enumerate(
                case.system.get_request_classes()
            ):
                clusters = tuple(
                    replication.request_class_latencies[request_class]
                    for replication in replications
                )
                interval = _prediction_interval(
                    clusters,
                    request_class,
                    case.statistic,
                    options,
                    options.bootstrap_seed_start
                    + case_index * len(case.system.get_request_classes())
                    + class_index,
                )
                observed = dict(case.observed_request_class_latencies)[
                    request_class
                ]
                predicted = interval.estimate
                error = predicted - observed
                relative_error = error / observed
                tolerance = max(
                    options.absolute_error_tolerance_seconds,
                    observed
                    * (
                        options.maximum_relative_underprediction
                        if error < 0.0
                        else options.maximum_relative_overprediction
                    ),
                )
                precise = (
                    interval.relative_half_width
                    <= options.maximum_relative_ci_half_width
                )
                passed = abs(error) <= tolerance and precise
                reason = None
                if abs(error) > tolerance:
                    direction = "underpredicts" if error < 0.0 else "overpredicts"
                    reason = (
                        f"Simulator {direction} observed {case.statistic} beyond "
                        "the configured predictive tolerance."
                    )
                elif not precise:
                    reason = "Simulation confidence interval is too imprecise."
                if reason:
                    reasons.append(f"{request_class}: {reason}")
                diagnostics.append(
                    RequestClassPredictionDiagnostic(
                        request_class=request_class,
                        statistic=case.statistic,
                        observed_seconds=observed,
                        predicted_seconds=predicted,
                        lower_bound_seconds=interval.lower_bound,
                        upper_bound_seconds=interval.upper_bound,
                        absolute_error_seconds=abs(error),
                        relative_error=relative_error,
                        relative_ci_half_width=interval.relative_half_width,
                        passed=passed,
                        reason=reason,
                    )
                )
                if request_class in errors and timeouts.get(request_class, 0.0) > 0.0:
                    failure_clusters = tuple(
                        replication.request_class_failures.get(
                            request_class,
                            (False,)
                            * len(
                                replication.request_class_latencies[request_class]
                            ),
                        )
                        for replication in replications
                    )
                    error_interval = replication_cluster_bootstrap_rate(
                        failure_clusters,
                        request_class=request_class,
                        resample_count=options.bootstrap_resample_count,
                        seed=(
                            options.bootstrap_seed_start
                            + len(cases) * len(case.system.get_request_classes())
                            + case_index * len(case.system.get_request_classes())
                            + class_index
                        ),
                        lower_tail_probability=(
                            1.0 - options.confidence_level
                        ) / 2.0,
                        upper_tail_probability=1.0
                        - (1.0 - options.confidence_level) / 2.0,
                        minimum_tail_draws=5,
                    )
                    error_difference = abs(
                        error_interval.estimate - errors[request_class]
                    )
                    error_rate_precise = (
                        error_interval.half_width
                        <= options.maximum_error_rate_ci_half_width
                    )
                    error_rate_passed = (
                        error_difference
                        <= options.maximum_absolute_error_rate_difference
                        and error_rate_precise
                    )
                    error_reason = None
                    if (
                        error_difference
                        > options.maximum_absolute_error_rate_difference
                    ):
                        error_reason = (
                            "Simulator terminal-failure rate differs from the "
                            "observation beyond tolerance."
                        )
                    elif not error_rate_precise:
                        error_reason = (
                            "Simulation failure-rate interval is too imprecise."
                        )
                    if error_reason:
                        reasons.append(f"{request_class}: {error_reason}")
                    error_rate_diagnostics.append(
                        RequestClassErrorRatePredictionDiagnostic(
                            request_class=request_class,
                            observed_rate=errors[request_class],
                            predicted_rate=error_interval.estimate,
                            lower_bound=error_interval.lower_bound,
                            upper_bound=error_interval.upper_bound,
                            absolute_error=error_difference,
                            ci_half_width=error_interval.half_width,
                            passed=error_rate_passed,
                            reason=error_reason,
                        )
                    )
        except Exception as error:  # Preserve a diagnostic rather than a false pass.
            reasons.append(f"Simulation validation failed: {error}")
        finally:
            case.system.configure_system_replicas(original_configuration)

        results.append(
            PredictiveValidationCaseResult(
                name=case.name,
                configuration=tuple(case.configuration),
                workload_fingerprint=case_workload_fingerprint,
                seeds=seeds,
                diagnostics=tuple(diagnostics),
                error_rate_diagnostics=tuple(error_rate_diagnostics),
                observed_request_class_error_rates=tuple(sorted(errors.items())),
                passed=not reasons and all(item.passed for item in diagnostics),
                reasons=tuple(reasons),
            )
        )

    if not target_case_present:
        global_reasons.append(
            "No held-out case matches the target workload and arrival process."
        )
    failed = tuple(case.name for case in results if not case.passed)
    if failed:
        global_reasons.append("Failed validation cases: " + ", ".join(failed))
    status = (
        PredictiveValidationStatus.VALIDATED
        if not global_reasons
        else PredictiveValidationStatus.REJECTED
    )
    return PredictiveValidationResult(
        status=status,
        target_experiment_fingerprint=target_fingerprint,
        model_structure_fingerprint=structure_fingerprint,
        simulator_semantics_version=SIMULATOR_SEMANTICS_VERSION,
        cases=tuple(results),
        reasons=tuple(global_reasons),
    )
