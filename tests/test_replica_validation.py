from types import SimpleNamespace

import pytest

from optimization import (
    FinalReplicaValidationError,
    validate_selected_replica_configuration,
)


class FakeValidationSystem:
    def __init__(self, p95_values, *, stable=True):
        self.p95_values = tuple(p95_values)
        self.stable = stable
        self.configuration = None
        self.simulation_calls = []

    def configure_system_replicas(self, configuration):
        self.configuration = tuple(configuration)

    def solve(self):
        return SimpleNamespace(get_stability=lambda: self.stable)

    def simulate_measurement_replications(
        self,
        *,
        warmup_duration,
        measurement_duration,
        seeds,
        workers,
    ):
        seeds = tuple(seeds)
        self.simulation_calls.append(
            {
                "configuration": self.configuration,
                "warmup_duration": warmup_duration,
                "measurement_duration": measurement_duration,
                "seeds": seeds,
                "workers": workers,
            }
        )
        return tuple(
            SimpleNamespace(
                seed=seed,
                replica_configuration=self.configuration,
                warmup_duration=warmup_duration,
                measurement_duration=measurement_duration,
                measured_request_counts={"checkout": 2_000},
                request_class_p95_latencies={
                    "checkout": self.p95_values[index]
                },
            )
            for index, seed in enumerate(seeds)
        )


def validate(system, seeds=tuple(range(10_000, 10_030)), **kwargs):
    return validate_selected_replica_configuration(
        system,
        (2, 1),
        warmup_duration=25.0,
        measurement_duration=1_000.0,
        request_class_slos={"checkout": 1.0},
        seeds=seeds,
        workers=4,
        confidence_level=0.95,
        excluded_seeds=tuple(range(30)),
        **kwargs,
    )


def test_final_validation_locks_configuration_and_accepts_upper_bound_below_slo():
    system = FakeValidationSystem([0.5] * 30)

    result = validate(system)

    assert result.passed
    assert result.replica_configuration == (2, 1)
    assert result.reliability.status.value == "reliably_feasible"
    assert result.reliability.seeds == tuple(range(10_000, 10_030))
    assert system.configuration == (2, 1)
    assert system.simulation_calls == [
        {
            "configuration": (2, 1),
            "warmup_duration": 25.0,
            "measurement_duration": 1_000.0,
            "seeds": tuple(range(10_000, 10_030)),
            "workers": 4,
        }
    ]


def test_final_validation_rejects_interval_that_overlaps_slo():
    system = FakeValidationSystem([0.9, 1.1] * 15)

    with pytest.raises(FinalReplicaValidationError) as caught:
        validate(system)

    assert caught.value.result is not None
    assert caught.value.result.reliability.status.value == "uncertain"
    assert not caught.value.result.passed


def test_final_validation_rejects_interval_entirely_above_slo():
    system = FakeValidationSystem([1.5] * 30)

    with pytest.raises(FinalReplicaValidationError) as caught:
        validate(system)

    assert caught.value.result.reliability.status.value == "reliably_infeasible"


def test_final_validation_rejects_reused_seed_before_simulating():
    system = FakeValidationSystem([0.5] * 30)

    with pytest.raises(ValueError, match="overlap optimizer seeds"):
        validate_selected_replica_configuration(
            system,
            (2, 1),
            warmup_duration=25.0,
            measurement_duration=1_000.0,
            request_class_slos={"checkout": 1.0},
            seeds=tuple(range(30)),
            excluded_seeds=tuple(range(30)),
        )

    assert system.configuration is None
    assert system.simulation_calls == []


def test_final_validation_requires_at_least_thirty_replications():
    system = FakeValidationSystem([0.5] * 30)

    with pytest.raises(ValueError, match="at least 30 independent seeds"):
        validate_selected_replica_configuration(
            system,
            (2, 1),
            warmup_duration=25.0,
            measurement_duration=1_000.0,
            request_class_slos={"checkout": 1.0},
            seeds=tuple(range(29)),
        )

    assert system.configuration is None
    assert system.simulation_calls == []


def test_final_validation_rejects_analytically_unstable_selection():
    system = FakeValidationSystem([0.5] * 30, stable=False)

    with pytest.raises(FinalReplicaValidationError, match="analytically unstable"):
        validate(system)

    assert system.configuration == (2, 1)
    assert system.simulation_calls == []
