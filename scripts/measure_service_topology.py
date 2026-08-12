#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import itertools
import json
import math
import re
import subprocess
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUEST_CLASS_FILES = [
    "metadata_read",
    "metadata_write",
    "datastore_read",
    "upload_heavy",
    "download_heavy",
    "auth",
    "semantic_query",
    "task_execution",
    "registry_metadata",
    "registry_image_io",
]

BACKEND_SERVICES = {"db", "redis", "minio"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def finite_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def parse_scalar(value: str) -> Any:
    value = value.strip()
    if value == "null":
        return None
    try:
        return float(value)
    except ValueError:
        return value


def parse_optimizer_input(path: Path) -> dict[str, Any]:
    namespace = None
    classes: dict[str, Any] = {}
    services: set[str] = set()
    current_class: str | None = None
    current_demand: dict[str, Any] | None = None

    for raw in path.read_text().splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if indent == 2 and stripped.startswith("namespace:"):
            namespace = stripped.split(":", 1)[1].strip()
        elif indent == 6 and stripped.endswith(":"):
            current_class = stripped[:-1]
            classes[current_class] = {
                "measured_reference_rps": None,
                "demands": [],
            }
            current_demand = None
        elif current_class and indent == 8:
            key, _, value = stripped.partition(":")
            if key in {"measured_reference_rps", "arrival_rate", "observed_p95_ms"}:
                classes[current_class][key] = parse_scalar(value)
        elif current_class and indent == 10 and stripped.startswith("- service:"):
            service = stripped.split(":", 1)[1].strip()
            current_demand = {"service": service}
            classes[current_class]["demands"].append(current_demand)
            services.add(service)
        elif current_demand and indent == 12:
            key, _, value = stripped.partition(":")
            if key in {"visits", "mu"}:
                current_demand[key] = parse_scalar(value)

    return {"namespace": namespace, "request_classes": classes, "services": sorted(services)}


def pod_workload_name(pod: dict[str, Any]) -> str:
    labels = pod.get("metadata", {}).get("labels", {})
    if labels.get("name"):
        return labels["name"]
    name = pod.get("metadata", {}).get("name", "")
    for suffix in ("-0", "-1", "-2"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    parts = name.split("-")
    if len(parts) >= 3 and re.fullmatch(r"[a-f0-9]{8,10}", parts[-2]):
        return "-".join(parts[:-2])
    if len(parts) >= 2:
        return "-".join(parts[:-1])
    return name


def discover_cluster(pods_json: dict[str, Any], objects_json: dict[str, Any]) -> dict[str, Any]:
    deployed_services: dict[str, Any] = {}
    workloads: dict[str, Any] = {}

    for item in objects_json.get("items", []):
        kind = item.get("kind")
        name = item.get("metadata", {}).get("name")
        if not name:
            continue
        if kind == "Service":
            ports = []
            for port in item.get("spec", {}).get("ports", []):
                ports.append({
                    "name": port.get("name"),
                    "port": port.get("port"),
                    "protocol": port.get("protocol"),
                    "targetPort": port.get("targetPort"),
                })
            deployed_services[name] = {"ports": ports}
        elif kind in {"Deployment", "StatefulSet"}:
            replicas = item.get("status", {}).get("readyReplicas", item.get("spec", {}).get("replicas", 1))
            workloads[name] = {"kind": kind, "replicas": replicas, "pods": []}

    for pod in pods_json.get("items", []):
        phase = pod.get("status", {}).get("phase")
        if phase not in {"Running", "Succeeded"}:
            continue
        name = pod.get("metadata", {}).get("name")
        workload = pod_workload_name(pod)
        containers = [c.get("name") for c in pod.get("spec", {}).get("containers", [])]
        has_sidecar = "istio-proxy" in containers
        pod_ip = pod.get("status", {}).get("podIP")
        workloads.setdefault(workload, {"kind": "Pod", "replicas": None, "pods": []})
        workloads[workload]["pods"].append({
            "name": name,
            "phase": phase,
            "pod_ip": pod_ip,
            "containers": containers,
            "has_istio_proxy": has_sidecar,
            "istio_telemetry_expected": has_sidecar and phase == "Running",
        })

    return {"services": deployed_services, "workloads": workloads}


def service_match_patterns(service: str) -> list[str]:
    aliases = {
        "db": ["db", "postgres", "postgresql", "postgis"],
        "redis": ["redis"],
        "minio": ["minio"],
        "solr": ["solr"],
        "keycloak": ["keycloak", "kc"],
        "quay": ["quay", "registry"],
        "ckan": ["ckan"],
        "ontop": ["ontop", "sparql"],
        "stelarapi": ["stelarapi", "stelar-api"],
        "datapusher": ["datapusher", "data-pusher"],
    }
    return aliases.get(service, [service])


def text_references_service(text: str, service: str) -> bool:
    lowered = text.lower()
    locator_words = r"(host|url|uri|endpoint|server|dsn|api|address|addr)"
    for alias in service_match_patterns(service):
        alias_l = alias.lower()
        escaped = re.escape(alias_l)
        hostlike = (
            rf"://{escaped}(?=[:/.]|$)|"
            rf"@{escaped}(?=[:/.]|$)|"
            rf"//{escaped}(?=[:/.]|$)|"
            rf"{escaped}\.stelar-lab|"
            rf"(?<![a-z0-9_-]){escaped}:\d+"
        )
        locator_assignment = rf"{locator_words}[^=\n]*=\s*(?:https?://|jdbc:|redis://|postgresql://)?{escaped}(?=[:/.]|$)"
        locator_key = rf"{escaped}[^=\n]*{locator_words}[^=\n]*="
        if re.search(hostlike, lowered) or re.search(locator_assignment, lowered) or re.search(locator_key, lowered):
            return True
    return False


def decode_secret_value(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True)
    except Exception:
        return ""
    try:
        return decoded.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def workload_config_references(config_json: dict[str, Any], deployed_services: set[str]) -> dict[tuple[str, str], dict[str, Any]]:
    configmaps: dict[str, dict[str, str]] = {}
    secrets: dict[str, dict[str, str]] = {}
    resource_refs: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"ConfigMap": set(), "Secret": set()})
    refs: dict[tuple[str, str], dict[str, Any]] = {}

    def add_ref(source: str, destination: str, evidence: str, runtime: bool) -> None:
        if source == destination:
            return
        body = refs.setdefault((source, destination), {"sources": set(), "runtime": False, "init_only": False})
        body["sources"].add(evidence)
        if runtime:
            body["runtime"] = True
        else:
            body["init_only"] = True

    for item in config_json.get("items", []):
        kind = item.get("kind")
        name = item.get("metadata", {}).get("name")
        if not name:
            continue
        if kind == "ConfigMap":
            data = item.get("data", {}) or {}
            configmaps[name] = {key: str(value) for key, value in data.items()}
        elif kind == "Secret":
            data = item.get("data", {}) or {}
            string_data = item.get("stringData", {}) or {}
            values = {key: decode_secret_value(value) for key, value in data.items()}
            values.update({key: str(value) for key, value in string_data.items()})
            secrets[name] = values

    for item in config_json.get("items", []):
        kind = item.get("kind")
        if kind not in {"Deployment", "StatefulSet"}:
            continue
        workload = item.get("metadata", {}).get("name")
        pod_spec = item.get("spec", {}).get("template", {}).get("spec", {})
        for volume in pod_spec.get("volumes", []) or []:
            if "configMap" in volume:
                ref_name = volume["configMap"].get("name")
                if ref_name:
                    resource_refs[workload]["ConfigMap"].add(ref_name)
            if "secret" in volume:
                ref_name = volume["secret"].get("secretName")
                if ref_name:
                    resource_refs[workload]["Secret"].add(ref_name)

        for container_kind, containers in (("container", pod_spec.get("containers", []) or []), ("init_container", pod_spec.get("initContainers", []) or [])):
            runtime = container_kind == "container"
            for container in containers:
                cname = container.get("name", "")
                for value in container.get("command", []) or []:
                    text = str(value)
                    for service in deployed_services:
                        if text_references_service(text, service):
                            add_ref(workload, service, f"{kind}/{workload} {container_kind} {cname} command references {service}", runtime)
                for value in container.get("args", []) or []:
                    text = str(value)
                    for service in deployed_services:
                        if text_references_service(text, service):
                            add_ref(workload, service, f"{kind}/{workload} {container_kind} {cname} args reference {service}", runtime)
                for env in container.get("env", []) or []:
                    env_name = str(env.get("name", ""))
                    if "value" in env:
                        text = f"{env_name}={env.get('value', '')}"
                        for service in deployed_services:
                            if text_references_service(text, service):
                                add_ref(workload, service, f"{kind}/{workload} {container_kind} {cname} env {env_name} references {service}", runtime)
                    value_from = env.get("valueFrom", {}) or {}
                    if "configMapKeyRef" in value_from:
                        ref = value_from["configMapKeyRef"]
                        ref_name = ref.get("name")
                        key = ref.get("key")
                        if ref_name:
                            resource_refs[workload]["ConfigMap"].add(ref_name)
                            text = f"{env_name} {key or ''}"
                            for service in deployed_services:
                                if text_references_service(text, service):
                                    add_ref(workload, service, f"{kind}/{workload} {container_kind} {cname} env {env_name} references ConfigMap/{ref_name} key {key}", runtime)
                    if "secretKeyRef" in value_from:
                        ref = value_from["secretKeyRef"]
                        ref_name = ref.get("name")
                        key = ref.get("key")
                        if ref_name:
                            resource_refs[workload]["Secret"].add(ref_name)
                            text = f"{env_name} {key or ''}"
                            for service in deployed_services:
                                if text_references_service(text, service):
                                    add_ref(workload, service, f"{kind}/{workload} {container_kind} {cname} env {env_name} references Secret/{ref_name} key {key}", runtime)
                for env_from in container.get("envFrom", []) or []:
                    if "configMapRef" in env_from and env_from["configMapRef"].get("name"):
                        resource_refs[workload]["ConfigMap"].add(env_from["configMapRef"]["name"])
                    if "secretRef" in env_from and env_from["secretRef"].get("name"):
                        resource_refs[workload]["Secret"].add(env_from["secretRef"]["name"])

    for workload, refs_for_workload in resource_refs.items():
        for cm_name in refs_for_workload["ConfigMap"]:
            for key, value in configmaps.get(cm_name, {}).items():
                text = f"{key}\n{value}"
                for service in deployed_services:
                    if text_references_service(text, service):
                        add_ref(workload, service, f"ConfigMap/{cm_name} key {key} mounted or referenced by {workload}", True)
        for secret_name in refs_for_workload["Secret"]:
            for key, value in secrets.get(secret_name, {}).items():
                text = f"{key}\n{value}"
                for service in deployed_services:
                    if text_references_service(text, service):
                        add_ref(workload, service, f"Secret/{secret_name} key {key} mounted or referenced by {workload}", True)

    return refs


def split_label_items(inner: str) -> list[str]:
    items, cur, quoted = [], [], False
    for ch in inner:
        if ch == '"':
            quoted = not quoted
        if ch == "," and not quoted:
            items.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        items.append("".join(cur))
    return items


def parse_promtool_output(output: str) -> list[dict[str, Any]]:
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line or "=>" not in line:
            continue
        labels_part, rest = line.split("=>", 1)
        labels: dict[str, str] = {}
        labels_part = labels_part.strip()
        if labels_part.startswith("{") and labels_part.endswith("}"):
            for piece in split_label_items(labels_part[1:-1]):
                if "=" in piece:
                    key, value = piece.split("=", 1)
                    labels[key.strip()] = value.strip().strip('"')
        value_text = rest.strip().split()[0]
        rows.append({"labels": labels, "value": finite_float(value_text)})
    return rows


def promtool_query(prom_namespace: str, pod: str, container: str, prom_url: str, promql: str) -> list[dict[str, Any]]:
    output = subprocess.check_output(
        [
            "kubectl",
            "exec",
            "-n",
            prom_namespace,
            pod,
            "-c",
            container,
            "--",
            "promtool",
            "query",
            "instant",
            prom_url,
            promql,
        ],
        text=True,
        stderr=subprocess.STDOUT,
        timeout=90,
    )
    return parse_promtool_output(output)


def class_measurement_window(result: dict[str, Any]) -> tuple[int, int] | None:
    measurement = result.get("metrics", {}).get("measurement", {})
    queried_at = measurement.get("queried_at")
    seconds = measurement.get("window_seconds")
    if not queried_at or not seconds:
        return None
    dt = datetime.fromisoformat(str(queried_at).replace("Z", "+00:00"))
    return int(seconds), int(dt.timestamp())


def load_class_results(results_dir: Path) -> dict[str, Any]:
    results = {}
    for cname in REQUEST_CLASS_FILES:
        path = results_dir / f"{cname}.json"
        if path.exists():
            results[cname] = load_json(path)
    return results


def add_edge(edge_map: dict[tuple[str, str], dict[str, Any]], source: str, destination: str, evidence_type: str, request_class: str | None = None, **values: Any) -> None:
    if not source or not destination or source == destination:
        return
    edge = edge_map.setdefault((source, destination), {
        "source": source,
        "destination": destination,
        "evidence_types": set(),
        "request_classes": set(),
        "observations": defaultdict(dict),
        "config_evidence": set(),
        "confidence": "low",
    })
    edge["evidence_types"].add(evidence_type)
    if request_class:
        edge["request_classes"].add(request_class)
    for key, value in values.items():
        if value is None:
            continue
        obs = edge["observations"][evidence_type]
        if key in {"observed_count", "observed_connections", "observed_bytes_sent", "observed_bytes_received"}:
            obs[key] = float(obs.get(key, 0.0)) + float(value)
        else:
            obs[key] = value


def collect_http_edges_from_results(class_results: dict[str, Any], edge_map: dict[tuple[str, str], dict[str, Any]], services: set[str]) -> dict[str, Any]:
    queries: dict[str, str] = {}
    for cname, result in class_results.items():
        measurement = result.get("metrics", {}).get("measurement", {})
        queries.update(measurement.get("queries", {}) or {})
        edge_rows = measurement.get("results", {}).get("edge_counts", [])
        for row in edge_rows if isinstance(edge_rows, list) else []:
            labels = row.get("labels", {})
            source = labels.get("source_workload")
            destination = labels.get("destination_workload")
            count = finite_float(row.get("value"))
            if source not in services or destination not in services or not count or count <= 0:
                continue
            add_edge(edge_map, source, destination, "observed_istio_http", cname, observed_count=count)
    return queries


def collect_prometheus_window_edges(args: argparse.Namespace, namespace: str, class_results: dict[str, Any], edge_map: dict[tuple[str, str], dict[str, Any]], services: set[str]) -> tuple[dict[str, str], list[str]]:
    query_templates = {
        "istio_http_edges": 'sum by (source_workload, destination_workload) (increase(istio_requests_total{{reporter="destination",source_workload_namespace="{ns}",destination_workload_namespace="{ns}"}}[{window}s] @ {end_ts}))',
        "istio_http_edges_with_destination_service": 'sum by (source_workload, destination_workload, destination_service) (increase(istio_requests_total{{reporter="destination",source_workload_namespace="{ns}",destination_workload_namespace="{ns}"}}[{window}s] @ {end_ts}))',
        "istio_tcp_connections": 'sum by (source_workload, destination_workload) (increase(istio_tcp_connections_opened_total{{reporter="destination",source_workload_namespace="{ns}",destination_workload_namespace="{ns}"}}[{window}s] @ {end_ts}))',
        "istio_tcp_sent_bytes": 'sum by (source_workload, destination_workload) (increase(istio_tcp_sent_bytes_total{{reporter="destination",source_workload_namespace="{ns}",destination_workload_namespace="{ns}"}}[{window}s] @ {end_ts}))',
        "istio_tcp_received_bytes": 'sum by (source_workload, destination_workload) (increase(istio_tcp_received_bytes_total{{reporter="destination",source_workload_namespace="{ns}",destination_workload_namespace="{ns}"}}[{window}s] @ {end_ts}))',
    }
    errors: list[str] = []
    rendered_queries: dict[str, str] = {}

    for cname, result in class_results.items():
        window = class_measurement_window(result)
        if not window:
            errors.append(f"{cname}: missing measurement window; skipped Prometheus window query")
            continue
        seconds, end_ts = window
        per_class: dict[str, list[dict[str, Any]]] = {}
        for name, template in query_templates.items():
            promql = template.format(ns=namespace, window=max(30, int(seconds)), end_ts=end_ts)
            rendered_queries[name] = template.format(ns=namespace, window="WINDOW", end_ts="END_TS")
            try:
                per_class[name] = promtool_query(args.prometheus_namespace, args.prometheus_pod, args.prometheus_container, args.prometheus_url, promql)
            except Exception as exc:
                per_class[name] = []
                errors.append(f"{cname} {name}: {exc}")

        for row in per_class.get("istio_http_edges", []):
            labels = row.get("labels", {})
            count = finite_float(row.get("value"))
            source = labels.get("source_workload")
            destination = labels.get("destination_workload")
            if source in services and destination in services and count and count > 0:
                add_edge(edge_map, source, destination, "observed_istio_http", cname, observed_count=count)

        tcp_totals: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        for query_name, target_key in (
            ("istio_tcp_connections", "observed_connections"),
            ("istio_tcp_sent_bytes", "observed_bytes_sent"),
            ("istio_tcp_received_bytes", "observed_bytes_received"),
        ):
            for row in per_class.get(query_name, []):
                labels = row.get("labels", {})
                value = finite_float(row.get("value"))
                source = labels.get("source_workload")
                destination = labels.get("destination_workload")
                if source in services and destination in services and value and value > 0:
                    tcp_totals[(source, destination)][target_key] = value
        for (source, destination), values in tcp_totals.items():
            add_edge(edge_map, source, destination, "observed_istio_tcp", cname, **values)

    return rendered_queries, errors


def config_edges_to_graph(
    refs: dict[tuple[str, str], dict[str, Any]],
    edge_map: dict[tuple[str, str], dict[str, Any]],
    optimizer: dict[str, Any],
) -> None:
    request_classes = optimizer["request_classes"]
    for (source, destination), body in refs.items():
        edge_classes = []
        for cname, cdef in request_classes.items():
            demanded = {demand["service"] for demand in cdef.get("demands", [])}
            if destination in demanded and (source in demanded or source not in optimizer["services"]):
                edge_classes.append(cname)
        add_edge(edge_map, source, destination, "inferred_config")
        edge = edge_map[(source, destination)]
        edge["request_classes"].update(edge_classes)
        edge["config_evidence"].update(body["sources"])
        edge["config_runtime"] = body.get("runtime", False)
        edge["config_init_only"] = body.get("init_only", False) and not body.get("runtime", False)


def finalize_edges(edge_map: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    finalized = []
    for edge in edge_map.values():
        evidence_types = sorted(edge["evidence_types"])
        if any(kind in evidence_types for kind in ("observed_istio_http", "observed_istio_tcp", "observed_native_backend")):
            confidence = "high"
        elif "inferred_config" in evidence_types and edge.get("config_runtime") and edge["request_classes"]:
            confidence = "medium"
        else:
            confidence = "low"

        out = {
            "source": edge["source"],
            "destination": edge["destination"],
            "evidence_types": evidence_types,
            "request_classes": sorted(edge["request_classes"]),
            "confidence": confidence,
        }
        if edge["observations"]:
            observations = {}
            for evidence_type, values in edge["observations"].items():
                observations[evidence_type] = {
                    key: round(value, 6) if isinstance(value, float) else value
                    for key, value in sorted(values.items())
                }
            out["observations"] = observations
        if edge["config_evidence"]:
            out["config_evidence"] = sorted(edge["config_evidence"])
        finalized.append(out)
    return sorted(finalized, key=lambda item: (item["source"], item["destination"]))


def build_unresolved(optimizer: dict[str, Any], edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unresolved = []
    for cname, cdef in optimizer["request_classes"].items():
        demanded = {demand["service"] for demand in cdef.get("demands", [])}
        for service in sorted(demanded):
            if service not in BACKEND_SERVICES:
                continue
            candidate_edges = [
                edge for edge in edges
                if edge["destination"] == service and cname in edge.get("request_classes", [])
            ]
            observed_edges = [
                edge for edge in candidate_edges
                if any(kind in edge.get("evidence_types", []) for kind in ("observed_istio_http", "observed_istio_tcp", "observed_native_backend"))
            ]
            if observed_edges:
                continue
            if not candidate_edges:
                unresolved.append({
                    "service": service,
                    "request_class": cname,
                    "evidence_type": "unresolved",
                    "reason": f"{service} demand appears in optimizer input, but no observed or config-inferred parent edge was found",
                })
            elif len(candidate_edges) > 1:
                unresolved.append({
                    "service": service,
                    "request_class": cname,
                    "evidence_type": "unresolved",
                    "candidate_sources": sorted(edge["source"] for edge in candidate_edges),
                    "reason": f"{service} activity is consistent with multiple config-inferred callers; native backend metrics did not uniquely attribute the caller",
                })
    return unresolved


def build_affinity_edges(optimizer: dict[str, Any]) -> list[dict[str, Any]]:
    pair_map: dict[tuple[str, str], dict[str, Any]] = {}
    for cname, cdef in optimizer["request_classes"].items():
        ref_rps = finite_float(cdef.get("measured_reference_rps")) or 1.0
        demands = cdef.get("demands", [])
        for a, b in itertools.combinations(sorted(demands, key=lambda d: d["service"]), 2):
            pair = tuple(sorted((a["service"], b["service"])))
            weight = ref_rps * min(float(a.get("visits") or 0.0), float(b.get("visits") or 0.0))
            edge = pair_map.setdefault(pair, {
                "service_a": pair[0],
                "service_b": pair[1],
                "weight": 0.0,
                "request_classes": set(),
                "reason": "co_visited_in_same_request_class",
            })
            edge["weight"] += weight
            edge["request_classes"].add(cname)
    out = []
    for edge in pair_map.values():
        out.append({
            "service_a": edge["service_a"],
            "service_b": edge["service_b"],
            "weight": round(edge["weight"], 6),
            "reason": edge["reason"],
            "request_classes": sorted(edge["request_classes"]),
        })
    return sorted(out, key=lambda item: (-item["weight"], item["service_a"], item["service_b"]))


def validate_demands(optimizer: dict[str, Any], edges: list[dict[str, Any]], unresolved: list[dict[str, Any]]) -> list[str]:
    graph_services = set()
    for edge in edges:
        graph_services.add(edge["source"])
        graph_services.add(edge["destination"])
    unresolved_pairs = {(item["request_class"], item["service"]) for item in unresolved}
    warnings = []
    for cname, cdef in optimizer["request_classes"].items():
        for demand in cdef.get("demands", []):
            service = demand["service"]
            if service not in graph_services and (cname, service) not in unresolved_pairs:
                warnings.append(f"{cname}/{service} appears in demands but not in directed_edges or unresolved_dependencies")
    return warnings


def yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9_.:/@+-]+", text):
        return text
    return json.dumps(text)


def emit_yaml(value: Any, indent: int = 0) -> list[str]:
    pad = " " * indent
    if isinstance(value, dict):
        lines = []
        for key, child in value.items():
            if isinstance(child, list) and not child:
                lines.append(f"{pad}{key}: []")
            elif isinstance(child, dict) and not child:
                lines.append(f"{pad}{key}: {{}}")
            elif isinstance(child, (dict, list)):
                lines.append(f"{pad}{key}:")
                lines.extend(emit_yaml(child, indent + 2))
            else:
                lines.append(f"{pad}{key}: {yaml_scalar(child)}")
        return lines
    if isinstance(value, list):
        lines = []
        for child in value:
            if isinstance(child, dict):
                if not child:
                    lines.append(f"{pad}- {{}}")
                    continue
                keys = list(child)
                first = keys[0]
                first_value = child[first]
                if isinstance(first_value, list) and not first_value:
                    lines.append(f"{pad}- {first}: []")
                elif isinstance(first_value, dict) and not first_value:
                    lines.append(f"{pad}- {first}: {{}}")
                elif isinstance(first_value, (dict, list)):
                    lines.append(f"{pad}- {first}:")
                    lines.extend(emit_yaml(first_value, indent + 4))
                else:
                    lines.append(f"{pad}- {first}: {yaml_scalar(first_value)}")
                for key in keys[1:]:
                    value_for_key = child[key]
                    if isinstance(value_for_key, list) and not value_for_key:
                        lines.append(f"{pad}  {key}: []")
                    elif isinstance(value_for_key, dict) and not value_for_key:
                        lines.append(f"{pad}  {key}: {{}}")
                    elif isinstance(value_for_key, (dict, list)):
                        lines.append(f"{pad}  {key}:")
                        lines.extend(emit_yaml(value_for_key, indent + 4))
                    else:
                        lines.append(f"{pad}  {key}: {yaml_scalar(value_for_key)}")
            else:
                lines.append(f"{pad}- {yaml_scalar(child)}")
        return lines
    return [f"{pad}{yaml_scalar(value)}"]


def write_topology_yaml(path: Path, graph: dict[str, Any]) -> None:
    path.write_text("\n".join(emit_yaml({"service_graph": graph})) + "\n")


def markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(item) for item in row) + " |")
    return lines


def write_report(
    path: Path,
    graph: dict[str, Any],
    cluster: dict[str, Any],
    config_refs: dict[tuple[str, str], dict[str, Any]],
    prom_queries: dict[str, str],
    prom_errors: list[str],
    validation_warnings: list[str],
    commands: list[str],
) -> None:
    lines = ["# STELAR Service Topology Measurement Report", ""]
    lines += [
        "## Summary",
        "",
        f"- Namespace: `{graph['namespace']}`",
        f"- Completeness: `{graph['completeness']}`",
        f"- Directed edges: `{len(graph['directed_edges'])}`",
        f"- Unresolved dependencies: `{len(graph['unresolved_dependencies'])}`",
        f"- Placement affinity edges: `{len(graph['placement_affinity_edges'])}`",
        "- Secret and ConfigMap values are not printed; evidence references only object names, keys, and matched service names.",
        "",
    ]

    sidecar_rows = []
    for workload, info in sorted(cluster["workloads"].items()):
        pods = info.get("pods", [])
        pod_names = ", ".join(pod["name"] for pod in pods) or "-"
        sidecar = ", ".join("yes" if pod["has_istio_proxy"] else "no" for pod in pods) or "-"
        expected = ", ".join("yes" if pod["istio_telemetry_expected"] else "no" for pod in pods) or "-"
        sidecar_rows.append([f"`{workload}`", pod_names, sidecar, expected])
    lines += ["## Istio Sidecar Coverage", ""]
    lines += markdown_table(["Workload", "Pods", "Has istio-proxy", "Istio telemetry expected"], sidecar_rows)
    lines.append("")

    http_edges = [edge for edge in graph["directed_edges"] if "observed_istio_http" in edge["evidence_types"]]
    tcp_edges = [edge for edge in graph["directed_edges"] if "observed_istio_tcp" in edge["evidence_types"]]
    inferred_edges = [edge for edge in graph["directed_edges"] if "inferred_config" in edge["evidence_types"]]

    def edge_rows(edges: list[dict[str, Any]], evidence_type: str) -> list[list[Any]]:
        rows = []
        for edge in edges:
            obs = edge.get("observations", {}).get(evidence_type, {})
            count = obs.get("observed_count", obs.get("observed_connections", ""))
            rows.append([
                f"`{edge['source']}`",
                f"`{edge['destination']}`",
                ", ".join(f"`{cls}`" for cls in edge.get("request_classes", [])) or "-",
                count,
                edge["confidence"],
            ])
        return rows

    lines += ["## Observed Istio HTTP Edges", ""]
    lines += markdown_table(["Source", "Destination", "Request classes", "Observed count", "Confidence"], edge_rows(http_edges, "observed_istio_http")) if http_edges else ["No non-zero Istio HTTP edges were available."]
    lines.append("")

    lines += ["## Observed Istio TCP Edges", ""]
    lines += markdown_table(["Source", "Destination", "Request classes", "Observed connections", "Confidence"], edge_rows(tcp_edges, "observed_istio_tcp")) if tcp_edges else ["No non-zero Istio TCP edges were available from the queried windows."]
    lines.append("")

    lines += ["## Config-Inferred Edges", ""]
    inferred_rows = []
    for edge in inferred_edges:
        evidence = "; ".join(edge.get("config_evidence", [])[:4])
        if len(edge.get("config_evidence", [])) > 4:
            evidence += "; ..."
        inferred_rows.append([
            f"`{edge['source']}`",
            f"`{edge['destination']}`",
            ", ".join(f"`{cls}`" for cls in edge.get("request_classes", [])) or "-",
            edge["confidence"],
            evidence,
        ])
    lines += markdown_table(["Source", "Destination", "Consistent request classes", "Confidence", "Redacted evidence"], inferred_rows) if inferred_rows else ["No config-inferred edges found."]
    lines.append("")

    lines += ["## Unresolved Dependencies", ""]
    unresolved_rows = [
        [
            f"`{item['request_class']}`",
            f"`{item['service']}`",
            ", ".join(f"`{source}`" for source in item.get("candidate_sources", [])) or "-",
            item["reason"],
        ]
        for item in graph["unresolved_dependencies"]
    ]
    lines += markdown_table(["Request class", "Service", "Candidate sources", "Reason"], unresolved_rows) if unresolved_rows else ["No unresolved dependencies."]
    lines.append("")

    lines += ["## Placement Affinity", "", "Placement affinity edges are undirected co-visit hints and are not used as directed service topology evidence.", ""]
    affinity_rows = [
        [
            f"`{edge['service_a']}`",
            f"`{edge['service_b']}`",
            edge["weight"],
            ", ".join(f"`{cls}`" for cls in edge.get("request_classes", [])),
        ]
        for edge in graph["placement_affinity_edges"][:20]
    ]
    lines += markdown_table(["Service A", "Service B", "Weight", "Request classes"], affinity_rows) if affinity_rows else ["No placement affinity edges."]
    lines.append("")

    lines += ["## Validation Against Optimizer Input", ""]
    lines += [f"- {warning}" for warning in validation_warnings] if validation_warnings else ["All demand services appear in directed edges or unresolved dependencies."]
    lines.append("")

    lines += ["## Exact PromQL Queries", ""]
    if prom_queries:
        for name, query in sorted(prom_queries.items()):
            lines.append(f"- `{name}`: `{query}`")
    else:
        lines.append("- Prometheus window queries were not executed in this run; existing isolated load-test result JSON was used for Istio HTTP edges.")
    if prom_errors:
        lines += ["", "### Prometheus Query Errors", ""]
        lines += [f"- `{error}`" for error in prom_errors]
    lines.append("")

    lines += ["## Exact Kubernetes Commands Used", ""]
    lines += [f"- `{command}`" for command in commands]
    lines.append("")

    lines += ["## Limitations", ""]
    lines += [
        "- Existing isolated load-test windows did not include historical native backend client-address samples, so DB and Redis activity is reported as unresolved when multiple config-inferred callers are possible.",
        "- Config-inferred edges prove configured dependencies, not per-request execution.",
        "- Istio may miss edges for traffic that bypasses sidecars, external front-door traffic, or protocols not represented in the queried metrics.",
        "- Placement affinity edges are intentionally separate from directed topology.",
        "",
    ]
    path.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--optimizer-input", default="workload.yaml")
    parser.add_argument("--results-dir", default="/home/ktistos/loadtest/results")
    parser.add_argument("--pods-json", default="/tmp/stelar-lab-pods.json")
    parser.add_argument("--objects-json", default="/tmp/stelar-lab-objects.json")
    parser.add_argument("--config-json", default="/tmp/stelar-lab-config.json")
    parser.add_argument("--output-yaml", default="service_topology.measured.yaml")
    parser.add_argument("--output-report", default="service_topology_measurement_report.md")
    parser.add_argument("--query-prometheus-windows", action="store_true")
    parser.add_argument("--prometheus-url", default="http://localhost:9090")
    parser.add_argument("--prometheus-namespace", default="monitoring")
    parser.add_argument("--prometheus-pod", default="prometheus-k8s-0")
    parser.add_argument("--prometheus-container", default="prometheus")
    args = parser.parse_args()

    optimizer = parse_optimizer_input(Path(args.optimizer_input))
    namespace = optimizer["namespace"] or "stelar-lab"
    class_results = load_class_results(Path(args.results_dir))
    pods_json = load_json(Path(args.pods_json))
    objects_json = load_json(Path(args.objects_json))
    config_json = load_json(Path(args.config_json))
    cluster = discover_cluster(pods_json, objects_json)

    deployed_services = set(cluster["services"]) | set(optimizer["services"])
    edge_map: dict[tuple[str, str], dict[str, Any]] = {}
    existing_queries = collect_http_edges_from_results(class_results, edge_map, deployed_services)
    prom_queries: dict[str, str] = dict(existing_queries)
    prom_errors: list[str] = []
    if args.query_prometheus_windows:
        rendered, prom_errors = collect_prometheus_window_edges(args, namespace, class_results, edge_map, deployed_services)
        prom_queries.update(rendered)

    config_refs = workload_config_references(config_json, deployed_services)
    config_edges_to_graph(config_refs, edge_map, optimizer)

    directed_edges = finalize_edges(edge_map)
    unresolved = build_unresolved(optimizer, directed_edges)
    affinity_edges = build_affinity_edges(optimizer)
    validation_warnings = validate_demands(optimizer, directed_edges, unresolved)

    graph = {
        "completeness": "mixed_observed_inferred",
        "namespace": namespace,
        "generated_at": now_iso(),
        "directed_edges": directed_edges,
        "unresolved_dependencies": unresolved,
        "placement_affinity_edges": affinity_edges,
    }

    commands = [
        "kubectl get deploy,sts,svc,pods,endpoints -n stelar-lab -o wide",
        "kubectl get virtualservice,gateway,destinationrule -A",
        "kubectl get pod -n stelar-lab -o json",
        "kubectl get deploy,sts,svc,endpoints -n stelar-lab -o json",
        "kubectl get deploy,sts,cm,secret -n stelar-lab -o json",
    ]
    if args.query_prometheus_windows:
        commands.append("kubectl exec -n monitoring prometheus-k8s-0 -c prometheus -- promtool query instant http://localhost:9090 '<PromQL>'")

    write_topology_yaml(Path(args.output_yaml), graph)
    write_report(Path(args.output_report), graph, cluster, config_refs, prom_queries, prom_errors, validation_warnings, commands)
    print(f"wrote {args.output_yaml} and {args.output_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
