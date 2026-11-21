"""Units, window alignment, evidence identity and real CLI loading path."""

import copy
import json
from pathlib import Path

import pytest
import yaml

from calibration.profile import experiment_fingerprints
from kubesizer_cli import _load_system
from performance_models import System, QueueingNetworkType
from performance_models.component_measurements import compile_component_measurements


EXAMPLES = Path(__file__).resolve().parents[1] / "examples/components"


def evidence(tmp_path):
    raw = yaml.safe_load((EXAMPLES / "evidence-worker.yaml").read_text())
    (tmp_path / "worker-observations.csv").write_text(
        (EXAMPLES / "worker-observations.csv").read_text()
    )
    return raw


def test_request_weighted_demand_and_provenance_survive_standard_loader(tmp_path):
    raw = evidence(tmp_path)
    path = tmp_path / "workload.yaml"
    path.write_text(yaml.safe_dump(raw))
    model, _ = _load_system(path)
    provenance = model.get_component_measurement_provenance()
    assert provenance["estimates"]["cpu_demand"]["value"] == pytest.approx(0.005)
    assert len(provenance["sources"]["counters"]["sha256"]) == 64
    first = experiment_fingerprints(model)
    observations = tmp_path / "worker-observations.csv"
    observations.write_text(
        observations.read_text().replace(",4.5,cpu_seconds", ",9,cpu_seconds")
    )
    changed, _ = _load_system(path)
    assert experiment_fingerprints(changed) != first
    assert model.get_workload_spec() == raw


@pytest.mark.parametrize(
    "mutation",
    [
        "overlap",
        "unaligned",
        "unit",
        "class",
        "negative",
        "zero_denominator",
        "wrong_destination",
    ],
)
def test_incompatible_evidence_is_rejected(tmp_path, mutation):
    raw = evidence(tmp_path)
    path = tmp_path / "worker-observations.csv"
    text = path.read_text()
    estimate = raw["component_measurements"]["estimates"]["cpu_demand"]
    if mutation == "overlap":
        text += text.splitlines()[1] + "\n"
    elif mutation == "unaligned":
        text = text.replace(
            "completed,worker,job,2026-01-01T00:00:00Z",
            "completed,worker,job,2026-01-01T00:00:01Z",
        )
    elif mutation == "unit":
        estimate["numerator"]["unit"] = "seconds"
    elif mutation == "class":
        estimate["denominator"]["request_class"] = "unrelated"
    elif mutation == "negative":
        text = text.replace(",0.5,cpu_seconds", ",-1,cpu_seconds")
    elif mutation == "zero_denominator":
        text = text.replace(",100,requests", ",0,requests").replace(
            ",900,requests", ",0,requests"
        )
    else:
        estimate["operation"] = "total"
        estimate["select"] = estimate.pop("numerator")
        estimate.pop("denominator")
        estimate["unit"] = "cpu_seconds"
        raw["traffic_model"]["service_times"]["worker"]["mean"]["unit"] = "cpu_seconds"
    path.write_text(text)
    with pytest.raises(ValueError):
        compile_component_measurements(raw, base_path=tmp_path)


def test_prometheus_adapter_sums_replica_cpu_before_dividing(tmp_path):
    raw = evidence(tmp_path)
    sources = raw["component_measurements"]["sources"]
    end = 1767225660  # 2026-01-01T00:01:00Z
    for name, values, unit, boundary in [
        ("cpu", [0.4, 0.6], "cpu_seconds", "local_cpu"),
        ("completed", [20, 180], "requests", "service_completion"),
    ]:
        path = tmp_path / (name + ".json")
        path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {
                                "metric": {"namespace": "exercise", "pod": str(i)},
                                "value": [end, str(v)],
                            }
                            for i, v in enumerate(values)
                        ],
                    },
                }
            )
        )
        sources[name] = {
            "format": "prometheus",
            "path": path.name,
            "metric": name,
            "component": "worker",
            "request_class": "job",
            "window_start": "2026-01-01T00:00:00Z",
            "window_end": "2026-01-01T00:01:00Z",
            "unit": unit,
            "boundary": boundary,
            "kind": "total",
            "query": "recorded increase query",
            "labels": {"namespace": "exercise"},
        }
    estimate = raw["component_measurements"]["estimates"]["cpu_demand"]
    estimate["numerator"]["source"] = "cpu"
    estimate["denominator"]["source"] = "completed"
    normalized, _ = compile_component_measurements(raw, base_path=tmp_path)
    assert normalized["traffic_model"]["service_times"]["worker"][
        "mean"
    ] == pytest.approx(1 / 200)


def test_observed_throughput_cannot_be_used_as_processing_capacity(tmp_path):
    raw = evidence(tmp_path)
    selector = raw["component_measurements"]["estimates"]["cpu_demand"]["denominator"]
    raw["component_measurements"]["estimates"]["throughput"] = {
        "operation": "rate",
        "unit": "requests_per_second",
        "select": selector,
    }
    raw["traffic_model"]["service_times"]["worker"] = {
        "distribution": "exponential",
        "rate": {"measurement": "throughput", "unit": "requests_per_second"},
    }
    with pytest.raises(ValueError, match="not processing capacity"):
        compile_component_measurements(raw, base_path=tmp_path)


def test_otlp_samples_measure_wall_time_not_cpu(tmp_path):
    raw = evidence(tmp_path)
    sources = raw["component_measurements"]["sources"]
    base = 1767225600000000000
    (tmp_path / "spans.json").write_text(
        json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "startTimeUnixNano": str(base + 1_000_000_000),
                                        "endTimeUnixNano": str(base + 1_020_000_000),
                                        "attributes": [
                                            {
                                                "key": "operation",
                                                "value": {"stringValue": "io"},
                                            }
                                        ],
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        )
    )
    sources["spans"] = {
        "format": "otlp_spans",
        "path": "spans.json",
        "metric": "duration",
        "component": "worker",
        "request_class": "job",
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:01:00Z",
        "unit": "seconds",
        "boundary": "io_phase_wall",
        "kind": "sample",
        "attributes": {"operation": "io"},
    }
    raw["component_measurements"]["estimates"]["io"] = {
        "operation": "samples",
        "unit": "seconds",
        "select": {
            k: sources["spans"][k]
            for k in ("metric", "component", "request_class", "unit", "boundary")
        },
    }
    raw["component_measurements"]["estimates"]["io"]["select"]["source"] = "spans"
    raw["traffic_model"]["service_times"]["worker"]["non_cpu_blocking_time"] = {
        "measurement": "io",
        "unit": "seconds",
    }
    model = System("otlp", raw, QueueingNetworkType.OPEN, workload_base_path=tmp_path)
    assert model.get_service_blocking_time_distribution_specs()[
        "worker"
    ].mean == pytest.approx(0.02)
    invalid_cpu = copy.deepcopy(raw)
    invalid_cpu["traffic_model"]["service_times"]["worker"] = {
        "measurement": "io",
        "unit": "seconds",
    }
    with pytest.raises(ValueError, match="CPU-attributed"):
        compile_component_measurements(invalid_cpu, base_path=tmp_path)
    sources["spans"]["boundary"] = "cpu_demand"
    raw["component_measurements"]["estimates"]["io"]["select"][
        "boundary"
    ] = "cpu_demand"
    with pytest.raises(ValueError, match="wall durations"):
        compile_component_measurements(raw, base_path=tmp_path)


def test_key_trace_adapter_preserves_order_and_sizes(tmp_path):
    raw = {
        "component_measurements": {
            "version": 1,
            "sources": {"keys": {"format": "cache_access_csv", "path": "keys.csv"}},
            "estimates": {
                name: {
                    "operation": op,
                    "unit": unit,
                    "source": "keys",
                    "component": "cache",
                    "request_class": "r",
                }
                for name, op, unit in [
                    ("sizes", "key_sizes", "key_sizes_bytes"),
                    ("trace", "key_accesses", "key_sequence"),
                ]
            },
        },
        "key_sizes_bytes": {"measurement": "sizes", "unit": "key_sizes_bytes"},
        "keys": {"measurement": "trace", "unit": "key_sequence"},
    }
    (tmp_path / "keys.csv").write_text(
        "component,request_class,window_start,window_end,timestamp,key,size_bytes\n"
        "cache,r,2026-01-01T00:00:00Z,2026-01-01T00:01:00Z,2026-01-01T00:00:02Z,b,2048\n"
        "cache,r,2026-01-01T00:00:00Z,2026-01-01T00:01:00Z,2026-01-01T00:00:01Z,a,1024\n"
    )
    result, _ = compile_component_measurements(raw, base_path=tmp_path)
    assert result["keys"] == ["a", "b"]
    assert result["key_sizes_bytes"] == {"a": 1024, "b": 2048}


def test_payload_samples_become_link_work_without_hiding_size_variation(tmp_path):
    raw = evidence(tmp_path)
    path = tmp_path / "payload.csv"
    path.write_text(
        "metric,component,request_class,window_start,window_end,value,unit,boundary,kind\n"
        "payload,link,job,2026-01-01T00:00:00Z,2026-01-01T00:01:00Z,1000,bytes,request_payload,sample\n"
        "payload,link,job,2026-01-01T00:00:00Z,2026-01-01T00:01:00Z,9000,bytes,request_payload,sample\n"
    )
    raw["component_measurements"]["sources"]["payload"] = {
        "format": "csv",
        "path": "payload.csv",
    }
    raw["component_measurements"]["estimates"]["transfer"] = {
        "operation": "transfer_samples",
        "unit": "seconds",
        "capacity_bytes_per_second": 1_000_000,
        "select": {
            "source": "payload",
            "metric": "payload",
            "component": "link",
            "request_class": "job",
            "unit": "bytes",
            "boundary": "request_payload",
        },
    }
    raw["delay"] = {"measurement": "transfer", "unit": "seconds"}
    result, _ = compile_component_measurements(raw, base_path=tmp_path)
    assert result["delay"]["samples"] == pytest.approx([0.001, 0.009])


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.yaml")), ids=lambda p: p.stem)
def test_every_component_example_uses_the_real_native_kernel(path):
    model, _ = _load_system(path)
    result = model.simulate_measurement_replications(
        warmup_duration=1, measurement_duration=3, maximum_drain_time=10, seeds=(123,)
    )[0]
    assert result.measured_request_counts == result.completed_measured_request_counts
    assert sum(result.measured_request_counts.values()) > 0


def test_fast_rejections_cannot_pass_a_latency_only_plan():
    raw = yaml.safe_load((EXAMPLES / "evidence-worker.yaml").read_text())
    raw["request_classes"]["job"].pop("maximum_terminal_failure_rate")
    model = System(
        "no-error-budget", raw, QueueingNetworkType.OPEN, workload_base_path=EXAMPLES
    )
    with pytest.raises(ValueError, match="maximum_terminal_failure_rate"):
        model.require_simulation_compatible_workload()
