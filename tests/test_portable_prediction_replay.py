import copy
import csv
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from characterization import build_characterized_system_variants, characterize_deployment
from scripts import replay_portable_predictions as replay


EXAMPLE = Path(__file__).parents[1] / 'examples/portable_evidence'
SETTINGS = dict(seeds=[101, 102, 103], warmup_seconds=1,
                measurement_seconds=2, maximum_drain_seconds=10)


@pytest.fixture
def case(tmp_path):
    package = tmp_path/'package'
    shutil.copytree(EXAMPLE, package)
    path = package/'workload.yaml'
    workload = yaml.safe_load(path.read_text())
    # Explicit evidence is for this synthetic fixture only, not an inferred
    # replacement fitted to any archived experimental outcome.
    workload['service_blocking_times'] = {
        name: {'distribution': 'deterministic', 'mean': .001}
        for name in workload['services']}
    for service in workload['services'].values():
        service['concurrency_limit_per_replica'] = 8
    path.write_text(yaml.safe_dump(workload))
    return dict(id='fixture', workload='package/workload.yaml',
        measurements='package/measurements.yaml', configuration={'api':2, 'database':3},
        arrival_process='stationary')


def test_replay_uses_production_model_and_service_named_configuration(case, tmp_path):
    inputs, actual = replay.prepare_case(case, tmp_path)
    expected = build_characterized_system_variants(inputs, characterize_deployment(inputs))
    assert [v.workload for v,_ in actual] == [v.workload for v,_ in expected]
    for _, system in actual:
        assert {system.get_service_by_id(i).get_name():n
                for i,n in enumerate(system.get_system_replicas())} == case['configuration']
    arrival = actual[0][0].workload['traffic_model']['entry_services']['browse']['api']
    assert arrival['sampling'] == 'block_bootstrap'
    assert arrival['block_duration'] > 0


def test_parallel_missing_evidence_abstains_before_simulating(case, tmp_path, monkeypatch):
    path = tmp_path/case['workload']
    workload = yaml.safe_load(path.read_text())
    workload['request_classes']['browse']['call_execution'] = 'parallel'
    del workload['service_blocking_times']['api']
    path.write_text(yaml.safe_dump(workload))
    def forbidden(*args, **kwargs):
        pytest.fail('Incomplete characterization reached simulation')
    monkeypatch.setattr(replay, 'cohort_summary', forbidden)
    result = replay.predict_case(case, tmp_path, SETTINGS)
    assert result['status'] == 'abstained'
    assert 'parallel downstream duration cannot be identified' in result['reason']
    assert result['variants'] == []
    assert result['production_certified'] is False


def test_legacy_frozen_model_is_not_portable_input(case, tmp_path):
    (tmp_path/case['workload']).write_text(yaml.safe_dump({
        'request_classes': {}, 'traffic_model': {'service_times': {}}}))
    result = replay.predict_case(case, tmp_path, SETTINGS)
    assert result['status'] == 'abstained'
    assert result['stage'] == 'production_characterization'


@pytest.mark.parametrize('configuration', [
    {'api':1}, {'api':1,'database':1,'unknown':1}, {'api':True,'database':1},
    {'api':0,'database':1}, {'api':1.5,'database':1},
])
def test_replica_configuration_is_not_silently_reordered_or_truncated(case, tmp_path, configuration):
    case['configuration'] = configuration
    with pytest.raises(ValueError, match='positive integer replicas'):
        replay.prepare_case(case, tmp_path)


@pytest.mark.parametrize('pattern', ['rate_step', 'changing_rate', None])
def test_transients_cannot_be_silently_bootstrapped(case, tmp_path, pattern):
    case['arrival_process'] = pattern
    result = replay.predict_case(case, tmp_path, SETTINGS)
    assert result['status'] == 'abstained'
    assert 'chronological replay' in result['reason']


def write_trace(path, rows, *, include_started=True):
    columns = ['request_sequence', 'request_class', 'scheduled_epoch', 'success']
    if include_started:
        columns.append('started_epoch')
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def test_arrivals_use_actual_starts_include_failures_and_preserve_ties(case, tmp_path):
    inputs, _ = replay.prepare_case(case, tmp_path)
    path = tmp_path/'trace.csv'
    rows = [dict(request_sequence=i,request_class='browse',scheduled_epoch=99,
                 started_epoch=t,success=ok)
            for i,(t,ok) in enumerate([(99,'True'),(101,'False'),(101,'True'),(102,'True'),(103,'True')])]
    write_trace(path, rows)
    replaced = replay.replace_arrivals(inputs, [path], start=100, end=103, expected_count=3)
    assert [s.timestamp for s in replaced.measurements.external_arrival_samples] == [1,1,2]
    assert replaced.fingerprint != inputs.fingerprint
    assert replaced.measurements.service_intervals == inputs.measurements.service_intervals
    assert replaced.workload == inputs.workload


def test_missing_actual_timestamps_never_falls_back_to_schedule(case, tmp_path):
    inputs, _ = replay.prepare_case(case, tmp_path)
    path = tmp_path/'trace.csv'
    write_trace(path, [], include_started=False)
    with pytest.raises(ValueError, match='actual started_epoch'):
        replay.replace_arrivals(inputs, [path], start=0, end=10, expected_count=1)


@pytest.mark.parametrize('timestamp', ['nan', 'inf', '-inf'])
def test_nonfinite_arrivals_rejected(case, tmp_path, timestamp):
    inputs, _ = replay.prepare_case(case, tmp_path)
    path = tmp_path/'trace.csv'
    write_trace(path,[dict(request_sequence=1,request_class='browse',scheduled_epoch=1,
                           started_epoch=timestamp,success='True')])
    with pytest.raises(ValueError, match='Nonfinite'):
        replay.replace_arrivals(inputs, [path], start=0, end=10, expected_count=1)


def test_count_and_duplicate_guards(case, tmp_path):
    inputs, _ = replay.prepare_case(case, tmp_path)
    path = tmp_path/'trace.csv'
    row = dict(request_sequence=1,request_class='browse',scheduled_epoch=1,started_epoch=1,success='True')
    write_trace(path,[row])
    with pytest.raises(ValueError, match='count mismatch'):
        replay.replace_arrivals(inputs,[path],start=0,end=10,expected_count=2)
    write_trace(path,[row,row])
    with pytest.raises(ValueError, match='duplicate'):
        replay.replace_arrivals(inputs,[path],start=0,end=10,expected_count=2)


@pytest.mark.parametrize('trend', ['growth_detected', 'inconclusive', 'no_material_drift_detected'])
def test_finite_cohort_never_becomes_stable_or_certified(case, tmp_path, monkeypatch, trend):
    inputs, variants = replay.prepare_case(case, tmp_path)
    system = variants[0][1]
    monkeypatch.setattr(replay, 'prepare_case', lambda *_: (inputs,variants))
    monkeypatch.setattr(system, 'simulate_measurement_replications', lambda **_: [
        SimpleNamespace(request_class_latencies={'browse':[.1,.2]})]*3)
    monkeypatch.setattr(replay, 'cohort_summary', lambda _: {'status':trend})
    result = replay.predict_case(case,tmp_path,SETTINGS)
    assert result['status'] == ('finite_window_complete' if trend=='no_material_drift_detected' else 'abstained')
    assert result['production_certified'] is False
    assert result['stationary_prediction_available'] is False
    assert 'stable' not in result
    assert result['variants'][0]['diagnostic_finite_window_means_ms']['browse'] == pytest.approx(150)


@pytest.mark.parametrize('key,value', [
    ('seeds',[1,1,2]), ('seeds',[1,2]), ('seeds',[True,2,3]),
    ('measurement_seconds',0), ('maximum_drain_seconds',float('nan')),
    ('warmup_seconds',-1),
])
def test_invalid_manifest_settings_fail(case,key,value):
    manifest = copy.deepcopy({**SETTINGS,'cases':[case]})
    manifest[key] = value
    with pytest.raises(ValueError):
        replay.validate_settings(manifest)
