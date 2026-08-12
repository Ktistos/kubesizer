from dataclasses import replace

import pytest

from calibration import (
    CalibrationOptions,
    MeasurementCalibrationError,
    calibrate_measurement_duration,
    initial_measurement_duration,
)
from native_backend import NativeMeasurementReplication
from performance_models import QueueingNetworkType, System


def make_system(read_gamma=2.0, rare_gamma=1.0):
    return System(
        "measurement-calibration",
        {
            "read": {"api": {"gamma": read_gamma, "mu": 20.0}},
            "rare": {"api": {"gamma": rare_gamma, "mu": 20.0}},
        },
        QueueingNetworkType.OPEN,
    )


def replication(seed, configuration, warmup, duration, counts):
    latencies = {
        name: tuple(1.0 + 0.001 * index for index in range(count))
        for name, count in counts.items()
    }
    arrivals = {
        name: tuple(
            warmup + duration * (index + 0.5) / count
            for index in range(count)
        )
        if count
        else ()
        for name, count in counts.items()
    }
    return NativeMeasurementReplication(
        seed=seed,
        replica_configuration=configuration,
        warmup_duration=warmup,
        measurement_duration=duration,
        stop_time=warmup + duration + 1.0,
        measured_request_counts=dict(counts),
        completed_measured_request_counts=dict(counts),
        request_class_p95_latencies={
            name: values[int((len(values) - 1) * 0.95)]
            for name, values in latencies.items()
            if values
        },
        request_class_latencies=latencies,
        request_class_arrival_times=arrivals,
        processed_event_count=100,
        external_arrival_count=sum(counts.values()),
        completed_request_count=sum(counts.values()),
        active_requests_at_end=0,
        queue_lengths_at_end=(0,),
        busy_replicas_at_end=(0,),
        tagged_requests_in_flight_at_end=0,
        maximum_drain_time=100.0,
    )


class FakeMeasurementSystem:
    def __init__(self, system, counts_per_class):
        self.system = system
        self.counts_per_class = counts_per_class
        self.calls = []

    def __getattr__(self, name):
        return getattr(self.system, name)

    def simulate_measurement_replications(
        self,
        *,
        warmup_duration,
        measurement_duration,
        seeds,
        workers,
        maximum_drain_time,
    ):
        configuration = tuple(self.system.get_system_replicas())
        self.calls.append((configuration, measurement_duration, tuple(seeds)))
        return tuple(
            replication(
                seed,
                configuration,
                warmup_duration,
                measurement_duration,
                self.counts_per_class,
            )
            for seed in seeds
        )


def options(**changes):
    base = CalibrationOptions(
        measurement_calibration_replications=4,
        target_samples_per_class=4,
        minimum_eligible_samples_per_class=8,
        minimum_nonempty_replications_per_class=3,
        measurement_duration_safety_factor=1.0,
        measurement_max_doublings=2,
        relative_ci_half_width_target=0.5,
        absolute_ci_half_width_target=0.5,
        measurement_stability_tolerance=0.5,
        ci_width_stability_tolerance=0.5,
        minimum_optimization_replications=4,
        maximum_optimization_replications=8,
        replication_batch_size=2,
        final_validation_replications=4,
        bootstrap_resample_count=1_000,
        minimum_bootstrap_tail_draws=5,
    )
    return replace(base, **changes)


def test_initial_duration_uses_slowest_required_external_class():
    assert initial_measurement_duration(
        make_system(read_gamma=4.0, rare_gamma=0.5),
        options(target_samples_per_class=10),
    ) == pytest.approx(20.0)


def test_calibration_stops_after_first_accepted_duration_pair():
    system = FakeMeasurementSystem(make_system(), {"read": 20, "rare": 20})
    minimum = tuple(system.get_minimum_stable_replicas())
    maximum = tuple(value + 1 for value in minimum)

    result = calibrate_measurement_duration(
        system,
        warmup_duration=3.0,
        representative_configurations=(minimum, maximum),
        request_class_slos={"read": 10.0, "rare": 10.0},
        options=options(),
        workers=2,
    )

    assert result.global_measurement_duration == result.candidate_durations[0]
    assert len(system.calls) == 2
    assert all(call[1] == result.candidate_durations[1] for call in system.calls)
    assert system.get_system_replicas() == [1]
    assert all(item.accepted_duration is not None for item in result.configurations)


def test_rare_class_with_insufficient_observations_fails_explicitly():
    system = FakeMeasurementSystem(make_system(), {"read": 20, "rare": 1})
    minimum = tuple(system.get_minimum_stable_replicas())
    selected_options = options()

    with pytest.raises(MeasurementCalibrationError) as captured:
        calibrate_measurement_duration(
            system,
            warmup_duration=0.0,
            representative_configurations=(minimum,),
            request_class_slos={"read": 10.0, "rare": 10.0},
            options=selected_options,
        )

    diagnostics = captured.value.diagnostics
    assert diagnostics.configurations[0].accepted_duration is None
    assert [call[1] for call in system.calls] == [
        diagnostics.candidate_durations[1],
        diagnostics.candidate_durations[-1],
    ]


def test_required_class_without_external_stream_is_rejected():
    with pytest.raises(MeasurementCalibrationError, match="positive stationary"):
        initial_measurement_duration(make_system(rare_gamma=0.0), options())
