"""Closed-loop validation of replica recommendations on real deployments.

Kubesizer owns the statistical analysis, not Kubernetes mutation or load-test
implementation.  A small adapter protocol lets a deployment-specific harness
apply configurations and return independent run observations while preserving
the portable core and making restoration explicit.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from metrics import (
    ClusterBootstrapP95Interval,
    IndependentReplicationRateInterval,
    independent_replication_rate_interval,
    replication_cluster_bootstrap_mean,
    replication_cluster_bootstrap_p95,
)


class ObservedConfigurationStatus(str, Enum):
    FEASIBLE = "FEASIBLE"
    INFEASIBLE = "INFEASIBLE"
    INCONCLUSIVE = "INCONCLUSIVE"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"


class RecommendationQualityStatus(str, Enum):
    MATCHES_EMPIRICAL_MINIMUM = "MATCHES_EMPIRICAL_MINIMUM"
    CONSERVATIVE = "CONSERVATIVE"
    UNDERPROVISIONED = "UNDERPROVISIONED"
    UNRESOLVED = "UNRESOLVED"
    NOT_TESTED = "NOT_TESTED"


@dataclass(frozen=True)
class ClosedLoopExperimentOptions:
    overall_alpha: float = 0.05
    bootstrap_resample_count: int = 20_000
    bootstrap_seed: int = 91_003
    minimum_replications: int = 5
    minimum_nonempty_replications_per_class: int = 5
    minimum_eligible_samples_per_class: int = 500
    minimum_bootstrap_tail_draws: int = 5


@dataclass(frozen=True)
class ObservedExperimentReplication:
    configuration: tuple[int, ...]
    seed: int
    run_id: str
    request_class_latencies: tuple[tuple[str, tuple[float, ...]], ...]
    request_class_failures: tuple[tuple[str, tuple[bool, ...]], ...] = ()
    request_class_censored: tuple[tuple[str, tuple[bool, ...]], ...] = ()

    @classmethod
    def from_mappings(
        cls,
        configuration,
        seed,
        run_id,
        request_class_latencies,
        *,
        request_class_failures=None,
        request_class_censored=None,
    ):
        configuration = _configuration(configuration)
        if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
            raise ValueError("seed must be a non-negative integer.")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run_id must be a nonempty string.")
        latency_mapping = {
            str(name): tuple(float(value) for value in values)
            for name, values in dict(request_class_latencies).items()
        }
        if not latency_mapping:
            raise ValueError("At least one request-class latency cohort is required.")
        if any(
            not name or any(not math.isfinite(value) or value < 0.0 for value in values)
            for name, values in latency_mapping.items()
        ):
            raise ValueError("Latency cohorts must be finite and non-negative.")
        failure_mapping = {}
        for name, values in dict(request_class_failures or {}).items():
            name = str(name)
            values = tuple(values)
            if any(value not in (False, True, 0, 1) for value in values):
                raise ValueError("Failure cohorts must contain binary outcomes.")
            if name not in latency_mapping or len(values) != len(latency_mapping[name]):
                raise ValueError(
                    "Failure and latency cohorts must align by request class."
                )
            failure_mapping[name] = tuple(bool(value) for value in values)
        censored_mapping = {}
        for name, values in dict(request_class_censored or {}).items():
            name = str(name)
            values = tuple(values)
            if any(value not in (False, True, 0, 1) for value in values):
                raise ValueError("Censor indicators must contain binary outcomes.")
            if name not in latency_mapping or len(values) != len(latency_mapping[name]):
                raise ValueError(
                    "Censor indicators and latency cohorts must align by request class."
                )
            censored_mapping[name] = tuple(bool(value) for value in values)
        return cls(
            configuration=configuration,
            seed=seed,
            run_id=run_id.strip(),
            request_class_latencies=tuple(sorted(latency_mapping.items())),
            request_class_failures=tuple(sorted(failure_mapping.items())),
            request_class_censored=tuple(sorted(censored_mapping.items())),
        )

    @property
    def latencies(self):
        return dict(self.request_class_latencies)

    @property
    def failures(self):
        return dict(self.request_class_failures)

    @property
    def censored(self):
        return dict(self.request_class_censored)


@dataclass(frozen=True)
class ObservedConfigurationEvidence:
    configuration: tuple[int, ...]
    objective_value: float
    status: ObservedConfigurationStatus
    replication_count: int
    run_ids: tuple[str, ...]
    seeds: tuple[int, ...]
    request_class_intervals: tuple[tuple[str, ClusterBootstrapP95Interval], ...]
    failure_rate_intervals: tuple[tuple[str, IndependentReplicationRateInterval], ...]
    censored_observation_counts: tuple[tuple[str, int], ...]
    reasons: tuple[str, ...]

    @property
    def intervals(self):
        return dict(self.request_class_intervals)


@dataclass(frozen=True)
class PredictionAgreement:
    configuration: tuple[int, ...]
    request_class: str
    statistic: str
    predicted_seconds: float
    observed_seconds: float
    observed_lower_bound_seconds: float
    observed_upper_bound_seconds: float
    absolute_error_seconds: float
    relative_error: float
    prediction_inside_observed_interval: bool


@dataclass(frozen=True)
class RecommendationQualityResult:
    status: RecommendationQualityStatus
    recommended_configuration: tuple[int, ...]
    recommended_objective_value: float
    recommendation_evidence: ObservedConfigurationEvidence | None
    configuration_evidence: tuple[ObservedConfigurationEvidence, ...]
    empirical_minimum_configuration: tuple[int, ...] | None
    empirical_minimum_objective_value: float | None
    absolute_cost_gap: float | None
    relative_cost_gap: float | None
    minimum_within_declared_space_certified: bool
    unresolved_lower_cost_configurations: tuple[tuple[int, ...], ...]
    prediction_agreement: tuple[PredictionAgreement, ...]
    overall_alpha: float
    per_tail_alpha: float
    guarantee: str

    @property
    def safe_in_observed_experiment(self):
        return self.status in {
            RecommendationQualityStatus.MATCHES_EMPIRICAL_MINIMUM,
            RecommendationQualityStatus.CONSERVATIVE,
        }


class ClosedLoopExperimentAdapter(Protocol):
    """Deployment-specific stateful operations required by the runner."""

    def snapshot_configuration(self): ...

    def apply_configuration(self, configuration: tuple[int, ...]): ...

    def run_replication(
        self,
        configuration: tuple[int, ...],
        seed: int,
    ) -> ObservedExperimentReplication: ...

    def restore_configuration(self, snapshot): ...


def _validate_options(options):
    if not math.isfinite(float(options.overall_alpha)) or not (
        0.0 < options.overall_alpha < 1.0
    ):
        raise ValueError("overall_alpha must lie strictly between zero and one.")
    for name in (
        "bootstrap_resample_count",
        "minimum_replications",
        "minimum_nonempty_replications_per_class",
        "minimum_eligible_samples_per_class",
        "minimum_bootstrap_tail_draws",
    ):
        value = getattr(options, name)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer.")
    if (
        isinstance(options.bootstrap_seed, bool)
        or not isinstance(options.bootstrap_seed, int)
        or options.bootstrap_seed < 0
    ):
        raise ValueError("bootstrap_seed must be a non-negative integer.")


def _configuration(value, *, service_count=None):
    result = tuple(value)
    if not result or any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0
        for item in result
    ):
        raise ValueError("Replica configurations must contain positive integers.")
    if service_count is not None and len(result) != service_count:
        raise ValueError("Every replica configuration must have the same length.")
    return result


def _cost(configuration, costs):
    return sum(
        replicas * cost for replicas, cost in zip(configuration, costs, strict=True)
    )


def _bootstrap_seed(base, configuration, request_class, kind):
    digest = hashlib.sha256(
        json.dumps(
            [base, list(configuration), request_class, kind],
            separators=(",", ":"),
        ).encode()
    ).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def _classify_configuration(
    configuration,
    replications,
    latency_slos,
    failure_rate_slos,
    *,
    latency_statistic,
    costs,
    options,
    per_tail_alpha,
):
    reasons = []
    intervals = []
    failure_intervals = []
    censored_counts = []
    if len(replications) < options.minimum_replications:
        reasons.append(
            f"requires at least {options.minimum_replications} independent runs"
        )
    seeds = tuple(item.seed for item in replications)
    run_ids = tuple(item.run_id for item in replications)
    if len(set(seeds)) != len(seeds):
        reasons.append("replication seeds are not unique within the configuration")
    if len(set(run_ids)) != len(run_ids):
        reasons.append("run identifiers are not unique within the configuration")

    for request_class, slo in latency_slos.items():
        try:
            clusters = tuple(item.latencies[request_class] for item in replications)
        except KeyError:
            reasons.append(f"missing latency cohort for {request_class!r}")
            continue
        total = sum(len(cluster) for cluster in clusters)
        nonempty = sum(bool(cluster) for cluster in clusters)
        censored_counts.append(
            (
                request_class,
                sum(
                    sum(
                        replication.censored.get(
                            request_class,
                            (False,) * len(replication.latencies[request_class]),
                        )
                    )
                    for replication in replications
                ),
            )
        )
        if total < options.minimum_eligible_samples_per_class:
            reasons.append(
                f"{request_class!r} has {total} eligible observations; "
                f"requires {options.minimum_eligible_samples_per_class}"
            )
            continue
        if nonempty < options.minimum_nonempty_replications_per_class:
            reasons.append(
                f"{request_class!r} has {nonempty} nonempty runs; requires "
                f"{options.minimum_nonempty_replications_per_class}"
            )
            continue
        bootstrap = (
            replication_cluster_bootstrap_mean
            if latency_statistic == "mean"
            else replication_cluster_bootstrap_p95
        )
        try:
            interval = bootstrap(
                clusters,
                request_class=request_class,
                slo=slo,
                resample_count=options.bootstrap_resample_count,
                seed=_bootstrap_seed(
                    options.bootstrap_seed,
                    configuration,
                    request_class,
                    latency_statistic,
                ),
                lower_tail_probability=per_tail_alpha,
                upper_tail_probability=1.0 - per_tail_alpha,
                minimum_tail_draws=options.minimum_bootstrap_tail_draws,
            )
        except ValueError as error:
            reasons.append(f"{request_class!r}: {error}")
        else:
            intervals.append((request_class, interval))

    for request_class, slo in failure_rate_slos.items():
        try:
            clusters = tuple(item.failures[request_class] for item in replications)
        except KeyError:
            reasons.append(f"missing failure cohort for {request_class!r}")
            continue
        try:
            interval = independent_replication_rate_interval(
                clusters,
                request_class=request_class,
                slo=slo,
                per_tail_alpha=per_tail_alpha,
            )
        except ValueError as error:
            reasons.append(f"{request_class!r}: {error}")
        else:
            failure_intervals.append((request_class, interval))

    if reasons:
        status = ObservedConfigurationStatus.INSUFFICIENT_EVIDENCE
    else:
        latency_pass = all(
            interval.upper_bound <= latency_slos[name]
            and not any(
                any(
                    replication.censored.get(
                        name,
                        (False,) * len(replication.latencies[name]),
                    )
                )
                for replication in replications
            )
            for name, interval in intervals
        )
        failure_pass = all(
            interval.upper_bound <= failure_rate_slos[name]
            and not any(
                any(
                    replication.censored.get(
                        name,
                        (False,) * len(replication.latencies[name]),
                    )
                )
                for replication in replications
            )
            for name, interval in failure_intervals
        )
        latency_fail = any(
            interval.lower_bound > latency_slos[name] for name, interval in intervals
        )
        failure_fail = any(
            interval.lower_bound > failure_rate_slos[name]
            for name, interval in failure_intervals
        )
        status = (
            ObservedConfigurationStatus.FEASIBLE
            if latency_pass and failure_pass
            else (
                ObservedConfigurationStatus.INFEASIBLE
                if latency_fail or failure_fail
                else ObservedConfigurationStatus.INCONCLUSIVE
            )
        )
    return ObservedConfigurationEvidence(
        configuration=configuration,
        objective_value=_cost(configuration, costs),
        status=status,
        replication_count=len(replications),
        run_ids=run_ids,
        seeds=seeds,
        request_class_intervals=tuple(intervals),
        failure_rate_intervals=tuple(failure_intervals),
        censored_observation_counts=tuple(sorted(censored_counts)),
        reasons=tuple(reasons),
    )


def evaluate_closed_loop_recommendation(
    recommended_configuration,
    replications,
    latency_slos,
    *,
    latency_statistic="mean",
    failure_rate_slos=None,
    replica_costs=None,
    declared_configuration_space=None,
    predicted_request_class_statistics=None,
    options=None,
):
    """Compare one recommendation with independent real-cluster load tests.

    ``minimum_within_declared_space_certified`` is true only when the caller
    supplies the complete finite comparison space and every cheaper state is
    conclusively infeasible.  Without that declaration the result is only the
    smallest *observed* passing configuration.
    """

    options = options or ClosedLoopExperimentOptions()
    _validate_options(options)
    if latency_statistic not in {"mean", "p95"}:
        raise ValueError("latency_statistic must be mean or p95.")
    latency_slos = {
        str(name): float(value) for name, value in dict(latency_slos).items()
    }
    failure_rate_slos = {
        str(name): float(value) for name, value in dict(failure_rate_slos or {}).items()
    }
    if not latency_slos:
        raise ValueError("At least one latency SLO is required.")
    if any(not math.isfinite(value) or value <= 0.0 for value in latency_slos.values()):
        raise ValueError("Latency SLOs must be finite and positive.")
    if any(
        not math.isfinite(value) or not 0.0 <= value < 1.0
        for value in failure_rate_slos.values()
    ):
        raise ValueError("Failure-rate SLOs must lie in [0, 1).")

    recommended = _configuration(recommended_configuration)
    service_count = len(recommended)
    costs = (
        (1.0,) * service_count
        if replica_costs is None
        else tuple(float(value) for value in replica_costs)
    )
    if len(costs) != service_count or any(
        not math.isfinite(value) or value <= 0.0 for value in costs
    ):
        raise ValueError("replica_costs must be finite and positive per service.")

    grouped = {}
    for replication in tuple(replications):
        if not isinstance(replication, ObservedExperimentReplication):
            raise TypeError(
                "replications must contain ObservedExperimentReplication values."
            )
        configuration = _configuration(
            replication.configuration,
            service_count=service_count,
        )
        grouped.setdefault(configuration, []).append(replication)
    if not grouped:
        raise ValueError("At least one observed configuration is required.")

    constraint_count = len(latency_slos) + len(failure_rate_slos)
    family_count = len(grouped) * constraint_count
    per_tail_alpha = options.overall_alpha / (2.0 * family_count)
    evidence = tuple(
        sorted(
            (
                _classify_configuration(
                    configuration,
                    tuple(items),
                    latency_slos,
                    failure_rate_slos,
                    latency_statistic=latency_statistic,
                    costs=costs,
                    options=options,
                    per_tail_alpha=per_tail_alpha,
                )
                for configuration, items in grouped.items()
            ),
            key=lambda item: (item.objective_value, item.configuration),
        )
    )
    by_configuration = {item.configuration: item for item in evidence}
    feasible = tuple(
        item for item in evidence if item.status is ObservedConfigurationStatus.FEASIBLE
    )
    empirical = feasible[0] if feasible else None
    recommendation = by_configuration.get(recommended)

    declared_space = None
    unresolved = []
    minimum_certified = False
    if declared_configuration_space is not None:
        declared_space = tuple(
            dict.fromkeys(
                _configuration(value, service_count=service_count)
                for value in declared_configuration_space
            )
        )
        if recommended not in declared_space:
            raise ValueError(
                "recommended_configuration is outside declared_configuration_space."
            )
        outside = set(by_configuration) - set(declared_space)
        if outside:
            raise ValueError(
                "Observed configurations lie outside declared_configuration_space: "
                f"{sorted(outside)}."
            )
    if empirical is not None and declared_space is not None:
        for configuration in declared_space:
            if _cost(configuration, costs) >= empirical.objective_value:
                continue
            candidate = by_configuration.get(configuration)
            if candidate is None or candidate.status is not (
                ObservedConfigurationStatus.INFEASIBLE
            ):
                unresolved.append(configuration)
        minimum_certified = not unresolved

    prediction_agreement = []
    for configuration, predictions in dict(
        predicted_request_class_statistics or {}
    ).items():
        configuration = _configuration(configuration, service_count=service_count)
        observed = by_configuration.get(configuration)
        if observed is None:
            continue
        for request_class, predicted in dict(predictions).items():
            interval = observed.intervals.get(request_class)
            if interval is None or dict(observed.censored_observation_counts).get(
                request_class, 0
            ):
                continue
            predicted = float(predicted)
            if not math.isfinite(predicted) or predicted < 0.0:
                raise ValueError("Predicted latency statistics must be non-negative.")
            error = predicted - interval.estimate
            prediction_agreement.append(
                PredictionAgreement(
                    configuration=configuration,
                    request_class=request_class,
                    statistic=latency_statistic,
                    predicted_seconds=predicted,
                    observed_seconds=interval.estimate,
                    observed_lower_bound_seconds=interval.lower_bound,
                    observed_upper_bound_seconds=interval.upper_bound,
                    absolute_error_seconds=abs(error),
                    relative_error=(
                        error / interval.estimate
                        if interval.estimate > 0.0
                        else (0.0 if error == 0.0 else math.inf)
                    ),
                    prediction_inside_observed_interval=(
                        interval.lower_bound - 1e-12
                        <= predicted
                        <= interval.upper_bound + 1e-12
                    ),
                )
            )

    recommended_cost = _cost(recommended, costs)
    if recommendation is None:
        status = RecommendationQualityStatus.NOT_TESTED
    elif recommendation.status is ObservedConfigurationStatus.INFEASIBLE:
        status = RecommendationQualityStatus.UNDERPROVISIONED
    elif recommendation.status is not ObservedConfigurationStatus.FEASIBLE:
        status = RecommendationQualityStatus.UNRESOLVED
    elif math.isclose(
        recommendation.objective_value,
        empirical.objective_value,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        status = RecommendationQualityStatus.MATCHES_EMPIRICAL_MINIMUM
    else:
        status = RecommendationQualityStatus.CONSERVATIVE

    if (
        empirical is None
        or recommendation is None
        or recommendation.status is not ObservedConfigurationStatus.FEASIBLE
    ):
        absolute_gap = None
        relative_gap = None
    else:
        absolute_gap = recommended_cost - empirical.objective_value
        relative_gap = absolute_gap / empirical.objective_value
    guarantee = (
        "Every lower-cost configuration in the declared finite experiment "
        "space was conclusively infeasible."
        if minimum_certified
        else (
            "The comparison identifies the smallest statistically passing "
            "configuration among tested cases only; it is not a global optimum."
        )
    )
    return RecommendationQualityResult(
        status=status,
        recommended_configuration=recommended,
        recommended_objective_value=recommended_cost,
        recommendation_evidence=recommendation,
        configuration_evidence=evidence,
        empirical_minimum_configuration=(
            None if empirical is None else empirical.configuration
        ),
        empirical_minimum_objective_value=(
            None if empirical is None else empirical.objective_value
        ),
        absolute_cost_gap=absolute_gap,
        relative_cost_gap=relative_gap,
        minimum_within_declared_space_certified=minimum_certified,
        unresolved_lower_cost_configurations=tuple(unresolved),
        prediction_agreement=tuple(prediction_agreement),
        overall_alpha=options.overall_alpha,
        per_tail_alpha=per_tail_alpha,
        guarantee=guarantee,
    )


def run_closed_loop_experiments(
    configurations,
    seeds,
    adapter: ClosedLoopExperimentAdapter,
):
    """Apply and measure configurations sequentially, restoring cluster state.

    This function intentionally does not parallelize configurations: concurrent
    mutations of one Kubernetes deployment would invalidate the experiment.
    Load generation and telemetry collection live in the supplied adapter.
    """

    configurations = tuple(_configuration(item) for item in configurations)
    if not configurations:
        raise ValueError("At least one experiment configuration is required.")
    service_count = len(configurations[0])
    configurations = tuple(
        _configuration(item, service_count=service_count) for item in configurations
    )
    if len(set(configurations)) != len(configurations):
        raise ValueError("Experiment configurations must be unique.")
    seeds = tuple(seeds)
    if not seeds or any(
        isinstance(seed, bool) or not isinstance(seed, int) or seed < 0
        for seed in seeds
    ):
        raise ValueError("Experiment seeds must be non-negative integers.")
    if len(set(seeds)) != len(seeds):
        raise ValueError("Experiment seeds must be unique.")

    snapshot = adapter.snapshot_configuration()
    results = []
    try:
        for configuration in configurations:
            adapter.apply_configuration(configuration)
            for seed in seeds:
                result = adapter.run_replication(configuration, seed)
                if not isinstance(result, ObservedExperimentReplication):
                    raise TypeError(
                        "adapter.run_replication must return "
                        "ObservedExperimentReplication."
                    )
                if result.configuration != configuration or result.seed != seed:
                    raise ValueError(
                        "Experiment adapter returned evidence for a different "
                        "configuration or seed."
                    )
                results.append(result)
    finally:
        adapter.restore_configuration(snapshot)
    return tuple(results)
