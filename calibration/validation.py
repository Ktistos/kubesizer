from dataclasses import dataclass

from .cutoff import WarmupCutoffNotFoundError, detect_ensemble_warmup_cutoff
from .ensemble import WarmupEnsembleSeries, average_warmup_replications
from .pilot import run_warmup_replications_for_configuration
from .sensitivity import PostWarmupStabilityResult, check_post_warmup_stability


class WarmupValidationError(RuntimeError):
    """Raised when fresh-seed validation cannot defend a global warm-up."""


@dataclass(frozen=True)
class ConfigurationWarmupValidation:
    replica_configuration: tuple[int, ...]
    seeds: tuple[int, ...]
    ensemble: WarmupEnsembleSeries
    required_warmup: float
    proposed_warmup: float
    post_warmup_check: PostWarmupStabilityResult
    passed: bool


@dataclass(frozen=True)
class WarmupValidationRound:
    round_index: int
    proposed_warmup: float
    configurations: tuple[ConfigurationWarmupValidation, ...]
    passed: bool


@dataclass(frozen=True)
class GlobalWarmupValidationResult:
    representative_configurations: tuple[tuple[int, ...], ...]
    replication_count_per_configuration: int
    seed_start: int
    rounds: tuple[WarmupValidationRound, ...]
    global_warmup: float
    passed: bool


def _normalize_configuration(configuration, service_count, label):
    configuration = tuple(configuration)
    if len(configuration) != service_count:
        raise ValueError(f"{label} must contain one replica count per service.")
    if any(not isinstance(value, int) or value <= 0 for value in configuration):
        raise ValueError(f"{label} replica counts must be positive integers.")
    return configuration


def build_representative_configurations(
    minimum_configuration,
    maximum_configuration,
    *,
    extra_configurations=(),
):
    """Build minimum, quartile intermediates, maximum, and supplied boundary cases."""
    minimum = tuple(minimum_configuration)
    maximum = _normalize_configuration(
        maximum_configuration,
        len(minimum),
        "maximum_configuration",
    )
    minimum = _normalize_configuration(
        minimum,
        len(maximum),
        "minimum_configuration",
    )
    if any(high < low for low, high in zip(minimum, maximum)):
        raise ValueError("maximum_configuration cannot be below the minimum.")

    configurations = [minimum]
    for numerator in (1, 2, 3):
        intermediate = tuple(
            low + ((high - low) * numerator) // 4
            for low, high in zip(minimum, maximum)
        )
        configurations.append(intermediate)
    configurations.append(maximum)
    configurations.extend(
        _normalize_configuration(
            configuration,
            len(minimum),
            "extra configuration",
        )
        for configuration in extra_configurations
    )

    unique = []
    seen = set()
    for configuration in configurations:
        if any(
            value < low or value > high
            for value, low, high in zip(configuration, minimum, maximum)
        ):
            raise ValueError("Representative configurations must lie within the bounds.")
        if configuration not in seen:
            seen.add(configuration)
            unique.append(configuration)
    return tuple(unique)


def validate_global_warmup(
    system,
    parameters,
    *,
    initial_warmup,
    maximum_configuration,
    bucket_width,
    window_length=10,
    validation_replication_count=10,
    validation_seed_start=200_000,
    extra_configurations=(),
    excluded_seeds=(),
    max_validation_rounds=3,
    cutoff_options=None,
    workers=1,
):
    """Validate and, when necessary, enlarge W on independent seed batches."""
    if initial_warmup < 0:
        raise ValueError("initial_warmup must be non-negative.")
    if not isinstance(validation_replication_count, int) or validation_replication_count <= 0:
        raise ValueError("validation_replication_count must be a positive integer.")
    if not isinstance(validation_seed_start, int) or validation_seed_start < 0:
        raise ValueError("validation_seed_start must be a non-negative integer.")
    if not isinstance(max_validation_rounds, int) or max_validation_rounds <= 0:
        raise ValueError("max_validation_rounds must be a positive integer.")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer.")

    representatives = build_representative_configurations(
        parameters.minimum_replica_configuration,
        maximum_configuration,
        extra_configurations=extra_configurations,
    )
    excluded_seeds = set(excluded_seeds)
    cutoff_options = dict(cutoff_options or {})
    cutoff_options["window_length"] = window_length
    safety_window = window_length * bucket_width
    proposed_warmup = float(initial_warmup)
    rounds = []

    for round_index in range(max_validation_rounds):
        configuration_results = []
        for configuration_index, configuration in enumerate(representatives):
            seed_offset = (
                round_index * len(representatives) + configuration_index
            ) * validation_replication_count
            seeds = tuple(
                validation_seed_start + seed_offset + index
                for index in range(validation_replication_count)
            )
            if excluded_seeds.intersection(seeds):
                raise ValueError("Validation seeds overlap calibration seeds.")

            ensemble_or_replications = run_warmup_replications_for_configuration(
                system,
                parameters,
                configuration,
                seeds,
                bucket_width=bucket_width,
                replication_id_start=seed_offset,
                workers=workers,
                aggregate=True,
            )
            ensemble = (
                ensemble_or_replications
                if isinstance(ensemble_or_replications, WarmupEnsembleSeries)
                else average_warmup_replications(ensemble_or_replications)
            )
            try:
                cutoff = detect_ensemble_warmup_cutoff(
                    ensemble,
                    **cutoff_options,
                )
            except WarmupCutoffNotFoundError as error:
                raise WarmupValidationError(
                    "Validation pilot is too short or nonstationary for "
                    f"configuration {configuration}; metric {error.metric_name} "
                    "did not settle."
                ) from error
            post_warmup = check_post_warmup_stability(
                ensemble,
                proposed_warmup,
                window_length=window_length,
                reference_tail_fraction=cutoff.reference_tail_fraction,
                relative_tolerance=cutoff.relative_tolerance,
                absolute_tolerances=cutoff_options.get("absolute_tolerances"),
                required_consecutive_windows=cutoff_options.get(
                    "required_consecutive_windows",
                    3,
                ),
            )
            passed = (
                cutoff.warmup_duration <= proposed_warmup
                and post_warmup.passed
            )
            configuration_results.append(
                ConfigurationWarmupValidation(
                    replica_configuration=configuration,
                    seeds=seeds,
                    ensemble=ensemble,
                    required_warmup=cutoff.warmup_duration,
                    proposed_warmup=proposed_warmup,
                    post_warmup_check=post_warmup,
                    passed=passed,
                )
            )

        round_passed = all(result.passed for result in configuration_results)
        rounds.append(
            WarmupValidationRound(
                round_index=round_index,
                proposed_warmup=proposed_warmup,
                configurations=tuple(configuration_results),
                passed=round_passed,
            )
        )
        if round_passed:
            return GlobalWarmupValidationResult(
                representative_configurations=representatives,
                replication_count_per_configuration=validation_replication_count,
                seed_start=validation_seed_start,
                rounds=tuple(rounds),
                global_warmup=proposed_warmup,
                passed=True,
            )

        required_warmup = max(
            result.required_warmup for result in configuration_results
        )
        proposed_warmup = max(proposed_warmup, required_warmup) + safety_window
        if proposed_warmup > parameters.pilot_horizon / 3.0:
            raise WarmupValidationError(
                "Representative configurations require W beyond H/3; extend "
                "the pilot horizon and repeat calibration."
            )

    raise WarmupValidationError(
        "Global warm-up did not pass fresh-seed validation within the allowed rounds."
    )
