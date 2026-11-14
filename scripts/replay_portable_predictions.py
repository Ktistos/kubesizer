"""Offline fixed-configuration diagnostics using production characterization.

This is not a sizing entry point. Use kubesizer for calibration, optimization
and fresh validation. Frozen simulator YAML is deliberately not accepted as
portable evidence. No fitting, safety thresholds or observations are adjusted.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import replace
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import yaml

from characterization import (
    build_characterized_system_variants,
    characterize_deployment,
    load_portable_sizing_inputs,
)
from characterization.models import ExternalArrivalSample
from native_backend import get_native_backend
from scripts.audit_simulation_semantics import cohort_summary


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def replace_arrivals(inputs, traces, *, start, end, expected_count):
    """Use actual starts, including failures, in one declared arrival window.

    Never fall back to scheduled times; do not manufacture or concatenate gaps
    across independent runs. Service characterization still uses low-load data.
    """
    if not all(math.isfinite(t) for t in (start, end)) or end <= start:
        raise ValueError("Arrival window must have finite increasing boundaries")
    if type(expected_count) is not int or expected_count <= 0:
        raise ValueError("expected_count must be a positive integer")
    classes = {c.name: c for c in inputs.workload.request_classes}
    samples, seen = [], set()
    for path in traces:
        with Path(path).open(newline="") as stream:
            reader = csv.DictReader(stream)
            if not {'started_epoch', 'request_class', 'request_sequence'} <= set(reader.fieldnames or ()):
                raise ValueError("Arrival trace requires actual started_epoch, request_class and request_sequence")
            for row in reader:
                # Unstarted scheduled work is not an arrival; its omission is
                # checked against the independently recorded started count.
                if not row['started_epoch']:
                    continue
                timestamp = float(row['started_epoch'])
                if not math.isfinite(timestamp):
                    raise ValueError("Nonfinite actual request-start timestamp")
                if not start <= timestamp < end:
                    continue
                name = row['request_class']
                if name not in classes:
                    raise ValueError(f"Unknown arrival request class {name!r}")
                identity = name, row['request_sequence']
                if not identity[1] or identity in seen:
                    raise ValueError("Missing/duplicate request sequence in arrival cohort")
                seen.add(identity)
                entries = classes[name].entry_services
                entry = row.get('entry_service') or (next(iter(entries)) if len(entries) == 1 else None)
                if entry not in entries:
                    raise ValueError("Actual arrivals need unambiguous entry-service attribution")
                samples.append(ExternalArrivalSample(timestamp-start, name, entry))
    if len(samples) != expected_count:
        raise ValueError(f"Actual-start cohort count mismatch: {len(samples)} != {expected_count}")
    samples.sort(key=lambda s: (s.timestamp, s.request_class, s.entry_service))
    binding = json.dumps([(s.timestamp, s.request_class, s.entry_service) for s in samples])
    return replace(inputs, measurements=replace(inputs.measurements, external_arrival_samples=tuple(samples)),
        fingerprint=hashlib.sha256((inputs.fingerprint+binding).encode()).hexdigest())


def prepare_case(case, base):
    allowed = {'id', 'workload', 'measurements', 'configuration', 'arrival_process', 'actual_arrivals'}
    if set(case) - allowed:
        raise ValueError(f"Unsupported replay fields: {sorted(set(case)-allowed)}")
    if case.get('arrival_process') != 'stationary':
        raise ValueError("This bootstrap replay supports stationary arrival processes only; transient histories require chronological replay")
    inputs = load_portable_sizing_inputs(base/case['workload'], base/case['measurements'])
    if 'actual_arrivals' in case:
        evidence = case['actual_arrivals']
        if set(evidence) != {'traces', 'start', 'end', 'expected_started_count'}:
            raise ValueError("Actual-arrival evidence requires traces, start, end and expected_started_count")
        inputs = replace_arrivals(inputs, [base/p for p in evidence['traces']],
            start=evidence['start'], end=evidence['end'], expected_count=evidence['expected_started_count'])
    configuration = case['configuration']
    if set(configuration) != set(inputs.workload.service_names) or any(
        type(value) is not int or value <= 0 for value in configuration.values()
    ):
        raise ValueError("Configuration must provide positive integer replicas for every service, by name")
    characterization = characterize_deployment(inputs)
    # Crucial: this is the production builder, not the lower-level variant
    # generator. It rejects incomplete blocking and external-boundary fits.
    variants = build_characterized_system_variants(inputs, characterization, evaluation_mode='simulation')
    for _, system in variants:
        system.configure_system_replicas(tuple(configuration[system.get_service_by_id(i).get_name()]
            for i in range(len(system.get_services()))))
    return inputs, variants


def predict_case(case, base, settings):
    result = {'id': case['id'], 'status': 'abstained', 'production_certified': False,
              'stationary_prediction_available': False, 'variants': []}
    try:
        inputs, variants = prepare_case(case, base)
    except (ValueError, OSError) as error:
        return {**result, 'reason': str(error), 'stage': 'production_characterization'}
    result['input_fingerprint'] = inputs.fingerprint
    for variant, system in variants:
        item = {'name': variant.name, 'assumptions': variant.assumptions,
                'status': 'abstained', 'model': variant.workload}
        result['variants'].append(item)
        if not system.solve().get_stability():
            item['reason'] = 'Necessary capacity gate failed'
            continue
        try:
            records = system.simulate_measurement_replications(
                seeds=settings['seeds'], warmup_duration=settings['warmup_seconds'],
                measurement_duration=settings['measurement_seconds'],
                maximum_drain_time=settings['maximum_drain_seconds'], workers=2)
            summary = cohort_summary(records)
            item['cohort_diagnostic'] = summary
            if summary['status'] == 'incomplete_or_failed':
                item['reason'] = 'Incomplete or failed measured cohort'
                continue
            item['diagnostic_finite_window_means_ms'] = {
                name: statistics.fmean(1000*statistics.fmean(r.request_class_latencies[name]) for r in records)
                for name in system.get_request_classes()}
            if summary['status'] != 'no_material_drift_detected':
                item['reason'] = 'Growing or inconclusive latency trend; no settled prediction'
                continue
            item['status'] = 'finite_window_complete'
        except (RuntimeError, ValueError) as error:
            item['reason'] = str(error)
    if all(v['status'] == 'finite_window_complete' for v in result['variants']):
        result['status'] = 'finite_window_complete'
    result['notice'] = ('Finite-window diagnostics only. No stability proof, optimizer search, '
                        'calibration validation or SLO feasibility certification was performed.')
    return result


def validate_settings(manifest):
    if set(manifest) != {'seeds', 'warmup_seconds', 'measurement_seconds', 'maximum_drain_seconds', 'cases'}:
        raise ValueError('Replay manifest has missing or unsupported fields')
    seeds = manifest['seeds']
    if len(seeds) < 3 or len(set(seeds)) != len(seeds) or any(type(s) is not int or s < 0 for s in seeds):
        raise ValueError('At least three distinct non-negative integer seeds are required')
    for key in ('warmup_seconds', 'measurement_seconds', 'maximum_drain_seconds'):
        value = manifest[key]
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (key != 'warmup_seconds' and value == 0):
            raise ValueError(f'Invalid {key}')
    ids = [c['id'] for c in manifest['cases']]
    if not ids or len(set(ids)) != len(ids) or any(not isinstance(i, str) or not i.strip() for i in ids):
        raise ValueError('Replay cases need unique nonempty IDs')


def source_bindings(path, manifest):
    paths = {path, Path(__file__).resolve(), get_native_backend().binary_path.resolve()}
    for folder in ('characterization', 'performance_models', 'native_backend', 'scripts'):
        paths.update((ROOT/folder).rglob('*.py'))
    paths.update((ROOT/'native_backend/cpp/src').glob('*.cpp'))
    for case in manifest['cases']:
        workload, measurement = (path.parent/case[k] for k in ('workload', 'measurements'))
        paths.update((workload, measurement))
        mapping = yaml.safe_load(measurement.read_text())
        for name, value in mapping.items():
            if name not in ('measurement_id', 'methodology_version'):
                paths.add(measurement.parent/value)
        if 'actual_arrivals' in case:
            paths.update(path.parent/p for p in case['actual_arrivals']['traces'])
    return {str(p.resolve()): digest(p) for p in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True, help='New diagnostic directory; never overwritten')
    args = parser.parse_args()
    path, out = args.manifest.resolve(), args.out.resolve()
    manifest = yaml.safe_load(path.read_text())
    validate_settings(manifest)
    bindings = source_bindings(path, manifest)
    out.mkdir(parents=True, exist_ok=False)
    def save(name, data):
        (out/name).write_text(json.dumps(data, indent=2, allow_nan=False)+'\n')
    save('bindings.json', bindings)
    save('protocol.json', manifest)
    results = []
    for case in manifest['cases']:
        result = predict_case(case, path.parent, manifest)
        results.append(result)
        save('predictions.json', results)
        print(f"{case['id']}: {result['status']} {result.get('reason', '')}", flush=True)
    for name, expected in bindings.items():
        if digest(name) != expected:
            raise RuntimeError(f'Bound input/source changed during replay: {name}')
    save('integrity.json', {'all_bound_sources_unchanged': True, 'production_certified': False})


if __name__ == '__main__':
    main()
