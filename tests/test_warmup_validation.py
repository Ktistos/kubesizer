from types import SimpleNamespace

import pytest

from calibration import build_representative_configurations, validate_global_warmup


def test_representative_configurations_cover_bounds_and_intermediates():
    configurations = build_representative_configurations(
        (1, 2),
        (9, 6),
        extra_configurations=((3, 3),),
    )

    assert configurations == (
        (1, 2),
        (3, 3),
        (5, 4),
        (7, 5),
        (9, 6),
    )


def test_representative_configurations_deduplicate_small_ranges():
    assert build_representative_configurations((1,), (2,)) == ((1,), (2,))


@pytest.mark.parametrize(
    "minimum,maximum",
    [
        ((2,), (1,)),
        ((1, 1), (2,)),
        ((0,), (2,)),
    ],
)
def test_representative_configuration_bounds_are_validated(minimum, maximum):
    with pytest.raises(ValueError):
        build_representative_configurations(minimum, maximum)


def test_extra_representative_must_stay_inside_bounds():
    with pytest.raises(ValueError, match="within the bounds"):
        build_representative_configurations(
            (1, 1),
            (4, 4),
            extra_configurations=((5, 2),),
        )


def test_fixed_validation_study_selects_maximum_cutoff_plus_safety(monkeypatch):
    parameters = SimpleNamespace(
        minimum_replica_configuration=(1,),
        pilot_horizon=300.0,
    )
    seen_batches = []

    def fake_runner(
        _system,
        _parameters,
        configuration,
        seeds,
        **_kwargs,
    ):
        seen_batches.append(tuple(seeds))
        return (SimpleNamespace(configuration=configuration, seeds=tuple(seeds)),)

    def fake_cutoff(ensemble, **_kwargs):
        is_first_round = ensemble.seeds[0] < 504
        required = 20.0 if ensemble.configuration == (2,) and is_first_round else 10.0
        return SimpleNamespace(
            warmup_duration=required,
            reference_tail_fraction=0.20,
            relative_tolerance=0.05,
        )

    monkeypatch.setattr(
        "calibration.validation.run_warmup_replications_for_configuration",
        fake_runner,
    )
    monkeypatch.setattr(
        "calibration.validation.average_warmup_replications",
        lambda replications: replications[0],
    )
    monkeypatch.setattr(
        "calibration.validation.detect_ensemble_warmup_cutoff",
        fake_cutoff,
    )
    monkeypatch.setattr(
        "calibration.validation.check_post_warmup_stability",
        lambda *_args, **_kwargs: SimpleNamespace(passed=True),
    )

    result = validate_global_warmup(
        object(),
        parameters,
        initial_warmup=10.0,
        maximum_configuration=(2,),
        bucket_width=1.0,
        window_length=10,
        validation_replication_count=2,
        validation_seed_start=500,
    )

    assert result.global_warmup == 30.0
    assert len(result.rounds) == 1
    assert result.rounds[0].passed
    assert seen_batches == [(500, 501), (502, 503)]
    assert len(set(seed for batch in seen_batches for seed in batch)) == 4


def test_validation_rejects_overlap_with_calibration_seeds(monkeypatch):
    parameters = SimpleNamespace(
        minimum_replica_configuration=(1,),
        pilot_horizon=300.0,
    )
    monkeypatch.setattr(
        "calibration.validation.run_warmup_replications_for_configuration",
        lambda *_args, **_kwargs: pytest.fail("overlapping seed must fail first"),
    )

    with pytest.raises(ValueError, match="overlap"):
        validate_global_warmup(
            object(),
            parameters,
            initial_warmup=10.0,
            maximum_configuration=(1,),
            bucket_width=1.0,
            validation_replication_count=2,
            validation_seed_start=700,
            excluded_seeds=(701,),
        )


def test_validation_requires_post_warmup_windows_to_pass(monkeypatch):
    parameters = SimpleNamespace(
        minimum_replica_configuration=(1,),
        pilot_horizon=300.0,
    )

    monkeypatch.setattr(
        "calibration.validation.run_warmup_replications_for_configuration",
        lambda _system, _parameters, configuration, seeds, **_kwargs: (
            SimpleNamespace(configuration=configuration, seeds=tuple(seeds)),
        ),
    )
    monkeypatch.setattr(
        "calibration.validation.average_warmup_replications",
        lambda replications: replications[0],
    )
    monkeypatch.setattr(
        "calibration.validation.detect_ensemble_warmup_cutoff",
        lambda *_args, **_kwargs: SimpleNamespace(
            warmup_duration=5.0,
            reference_tail_fraction=0.20,
            relative_tolerance=0.05,
        ),
    )
    monkeypatch.setattr(
        "calibration.validation.check_post_warmup_stability",
        lambda *_args, **_kwargs: SimpleNamespace(passed=False),
    )

    result = validate_global_warmup(
        object(),
        parameters,
        initial_warmup=10.0,
        maximum_configuration=(1,),
        bucket_width=1.0,
        validation_replication_count=2,
        validation_seed_start=900,
    )

    assert not result.passed
    assert not result.rounds[0].passed
    assert not result.rounds[0].configurations[0].passed
