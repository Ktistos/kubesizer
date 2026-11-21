"""Provision independent four-drive and eight-drive MinIO sizing layouts."""

from __future__ import annotations

import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_minio_pilot as pilot
from scripts import characterize_minio_fragments as fragments

CAMPAIGN = "mis1"
BUCKET = "mis1-sizing-data"
LAYOUTS = (4, 8)
NODES = ("skube06", "skube07", "skube08", "skube09")
MEMBER_LABEL = "kubesizer.campaign"


def members(size):
    return tuple(f"ms{size}m{index}" for index in range(1, size + 1))


def kube(*arguments, timeout=180):
    return pilot.kube("-n", pilot.NAMESPACE, *arguments, timeout=timeout)


def get(kind, name):
    value = kube("get", kind, name, "--ignore-not-found", "-o", "json")
    return json.loads(value) if value.strip() else None


def documents(size):
    with tempfile.TemporaryDirectory() as folder:
        pilot.render(Path(folder))
        prototypes = list(yaml.safe_load_all((Path(folder) / "workload.yaml").read_text()))
    service = next(row for row in prototypes if row["kind"] == "Service")
    statefulset = next(row for row in prototypes if row["kind"] == "StatefulSet")
    resources = []
    for index, member in enumerate(members(size)):
        labels = {MEMBER_LABEL: CAMPAIGN, "app": member, "kubesizer.layout": str(size)}
        metadata = {"name": member, "namespace": pilot.NAMESPACE, "labels": labels}
        deployed_service = deepcopy(service)
        deployed_service.update(metadata=metadata)
        deployed_service["spec"]["selector"] = labels
        deployed_set = deepcopy(statefulset)
        deployed_set.update(metadata=metadata)
        spec = deployed_set["spec"]
        spec.update(serviceName=member, replicas=0, selector={"matchLabels": labels})
        spec["template"]["metadata"] = {"labels": labels}
        template = spec["template"]["spec"]
        template["nodeSelector"]["kubernetes.io/hostname"] = NODES[index % len(NODES)]
        template["automountServiceAccountToken"] = False
        minio, observer = template["containers"]
        minio["args"] = ["server", "--address", ":9000", f"http://ms{size}m{{1...{size}}}:9000/data"]
        if index == 3:
            observer["resources"]["limits"].update(cpu="500m", memory="192Mi")
        resources.extend([deployed_service, deployed_set])
    return resources


def assert_isolated():
    state = subprocess.run(
        ["systemctl", "is-active", pilot.common.COMPARISON],
        capture_output=True, text=True,
    ).stdout.strip()
    if state != "inactive":
        raise RuntimeError(f"Comparison campaign is {state!r}")
    pods = json.loads(kube("get", "pods", "-o", "json"))["items"]
    if pods:
        raise RuntimeError("The MinIO pilot namespace already has running or pending pods")
    for member in pilot.MEMBERS:
        original = get("statefulset", member)
        if original is None or original["spec"]["replicas"] != 0:
            raise RuntimeError(f"Legacy MinIO member is active or absent: {member}")


def install(output):
    assert_isolated()
    plans = {size: documents(size) for size in LAYOUTS}
    for size in LAYOUTS:
        for member in members(size):
            if get("statefulset", member) or get("service", member):
                raise RuntimeError(f"Trial resource name is already in use: {member}")
        declaration = output / f"layout-{size}.yaml"
        declaration.write_text(yaml.safe_dump_all(plans[size], sort_keys=False))
        kube("create", "--dry-run=server", "-f", str(declaration))
    pilot.write(output / "resources-owned.json", {"used_layouts": list(LAYOUTS), "created_at": pilot.now()})
    for size in LAYOUTS:
        kube("create", "-f", str(output / f"layout-{size}.yaml"))


def owned(member):
    resource = get("statefulset", member)
    if resource and resource["metadata"]["labels"].get(MEMBER_LABEL) != CAMPAIGN:
        raise RuntimeError(f"Unexpected ownership for {member}")
    return resource


def stop(size):
    targets = [name for name in members(size) if owned(name)]
    if targets:
        kube("scale", "statefulset", *targets, "--replicas=0")
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        running = json.loads(kube(
            "get", "pods", "-l", f"{MEMBER_LABEL}={CAMPAIGN},kubesizer.layout={size}", "-o", "json",
        ))["items"]
        if not running:
            return
        time.sleep(3)
    raise RuntimeError(f"Layout {size} pods still active after scale down")


def finish(output):
    if not (output / "resources-owned.json").is_file() or (output / "cleanup.json").exists():
        return
    for size in LAYOUTS:
        stop(size)
    pilot.write(output / "cleanup.json", {"pods_running": 0, "completed_at": pilot.now()})


def get_admin_info(size):
    shell = (
        'mc alias set mis1 http://127.0.0.1:9000 "$MINIO_ROOT_USER" '
        '"$MINIO_ROOT_PASSWORD" >/dev/null && mc --json admin info mis1'
    )
    return json.loads(kube(
        "exec", f"{members(size)[0]}-0", "-c", "minio", "--", "sh", "-c", shell,
        timeout=45,
    ))


def verify_layout(size, record, placement):
    info = record["info"]
    backend = info["backend"]
    expected = {
        "backendType": "Erasure", "onlineDisks": size, "offlineDisks": 0,
        "standardSCParity": 2, "totalSets": [size // 4], "totalDrivesPerSet": [4],
    }
    if any(backend.get(field) != value for field, value in expected.items()):
        raise ValueError(f"Actual erasure topology differs from the declared layout of {size}")
    sizes = defaultdict(list)
    names = set(members(size))
    for server in info["servers"]:
        name = server["endpoint"].split(":", 1)[0]
        drives = server["drives"]
        if name not in names or server["state"] != "online" or len(drives) != 1:
            raise ValueError("Unexpected member state, identity or drive count")
        names.remove(name)
        drive = drives[0]
        if drive["state"] != "ok" or drive["pool_index"] != 0:
            raise ValueError("A drive is unhealthy or in an unexpected server pool")
        sizes[drive["set_index"]].append(name)
    if names or set(sizes) != set(range(size // 4)):
        raise ValueError("Not all declared drives and sets were observed")
    for group in sizes.values():
        if len(group) != 4 or {placement[name] for name in group} != set(NODES):
            raise ValueError("Every erasure set must span the same four independent worker nodes")
    return dict(backend=backend, members_by_set=dict(sizes), placement=placement)


def start(size, output, *, instrumented=None, digest=None, client_limits=None):
    for name in members(size):
        resource = owned(name)
        if resource is None or resource["spec"]["replicas"] != 0:
            raise RuntimeError(f"Expected an installed, idle {name}")
        if instrumented is not None:
            parts = fragments.patched_parts(resource)
            for container in parts["containers"]:
                if client_limits and name == members(size)[3] and container["name"] == "observer":
                    container["resources"]["limits"].update(client_limits)
                if container["name"] == "minio":
                    container["command"] = [
                        "sh", "-c",
                        f"while [ ! -x {fragments.MOUNT}/minio ]; do sleep 1; done; "
                        f"export KUBESIZER_PHASE_BUCKET={BUCKET}; exec {fragments.MOUNT}/minio \"$@\"",
                        "minio",
                    ]
            fragments.patch(name, 0, parts)
    kube("scale", "statefulset", *members(size), "--replicas=1")
    if instrumented is not None:
        deadline = time.monotonic() + 270
        while time.monotonic() < deadline:
            pods = json.loads(kube("get", "pods", "-l", f"kubesizer.layout={size}", "-o", "json"))["items"]
            active = {
                row["metadata"]["name"] for row in pods
                if any(c["name"] == "observer" and "running" in c["state"]
                       for c in row["status"].get("containerStatuses", []))
            }
            if active == {f"{name}-0" for name in members(size)}:
                break
            time.sleep(4)
        else:
            raise RuntimeError(f"Observer startup timed out for the {size}-drive layout")
        with ThreadPoolExecutor(max_workers=4) as workers:
            hashes = dict(workers.map(lambda name: fragments.upload(name, instrumented, digest), members(size)))
        pilot.write(output / f"instrumented-binaries-{size}.json", hashes)
    with ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(
            lambda name: kube("rollout", "status", f"statefulset/{name}", "--timeout=480s", timeout=500),
            members(size),
        ))
    pods = json.loads(kube("get", "pods", "-l", f"kubesizer.layout={size}", "-o", "json"))["items"]
    placement = {row["metadata"]["labels"]["app"]: row["spec"]["nodeName"] for row in pods}
    if client_limits:
        client_pod = next(row for row in pods if row["metadata"]["labels"]["app"] == members(size)[3])
        observer = next(row for row in client_pod["spec"]["containers"] if row["name"] == "observer")
        actual_limits = observer["resources"]["limits"]
        if actual_limits != client_limits:
            raise RuntimeError(f"Actual observer limits differ for layout {size}: {actual_limits}")
        pilot.write(output / f"client-limits-{size}.json", {"pod": client_pod["metadata"]["name"], "limits": actual_limits})
    deadline = time.monotonic() + 100
    while True:
        try:
            record = get_admin_info(size)
            verified = verify_layout(size, record, placement)
            break
        except (RuntimeError, ValueError, KeyError) as error:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"MinIO topology was not ready for layout {size}: {error}") from error
            time.sleep(4)
    pilot.write(output / f"layout-{size}-verified.json", verified)
    pilot.write(output / f"layout-{size}-admin-info.json", pilot.redact(record))
    return verified


def preflight(output):
    output.mkdir(parents=True, exist_ok=False)
    status = {"status": "PROVISIONING", "started_at": pilot.now()}
    pilot.write(output / "status.json", status)
    try:
        install(output)
        for size in LAYOUTS:
            status.update(status="CHECKING_LAYOUT", layout=size, checked_at=pilot.now())
            pilot.write(output / "status.json", status)
            start(size, output)
            stop(size)
        status.update(status="COMPLETED", completed_at=pilot.now())
    except BaseException as error:
        status.update(status="FAILED", error=f"{type(error).__name__}: {error}", failed_at=pilot.now())
        raise
    finally:
        pilot.write(output / "status.json", status)
        finish(output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--finish", action="store_true")
    args = parser.parse_args()
    if args.finish:
        finish(args.output.resolve())
    else:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
        preflight(args.output.resolve())
