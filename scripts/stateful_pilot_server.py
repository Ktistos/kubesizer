"""Controlled persistent FIFO worker for the sfp1 experiment, not a DB emulator.

Each member owns a SQLite database. Replication is application-level HTTP;
there are no elections, recovery, transactions across members or consistency
guarantees. Local worker occupancy is explicitly instrumented experimental
input, rather than inferred from generic Kubernetes CPU telemetry.
"""

import hashlib
import json
import os
import random
import sqlite3
import threading
import time
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


class Worker:
    def __init__(self, name, database, *, targets=(), means=(0.01, 0.02)):
        self.name, self.targets, self.means = name, tuple(targets), means
        self.condition = threading.Condition()
        self.queue, self.active = deque(), False
        self.lock = threading.Lock()
        self.events = deque(maxlen=200_000)
        self.sequence, self.generation = 0, 0
        self.counts = Counter()
        self.rng = random.Random(1)
        self.calls = ThreadPoolExecutor(max_workers=128)
        self.db = sqlite3.connect(database, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS kv (k INTEGER PRIMARY KEY, v TEXT)")
        self.db.executemany(
            "INSERT OR IGNORE INTO kv VALUES (?, ?)",
            ((i, "initial") for i in range(1024)),
        )
        self.db.commit()

    def event(self, kind, **values):
        with self.lock:
            self.sequence += 1
            self.events.append(
                dict(
                    sequence=self.sequence,
                    epoch=time.time(),
                    type=kind,
                    service=self.name,
                    **values,
                )
            )

    def status(self):
        with self.condition:
            queue, active = len(self.queue), int(self.active)
        with self.lock:
            return dict(
                service=self.name,
                queued=queue,
                active=active,
                generation=self.generation,
                last_sequence=self.sequence,
                counters=dict(self.counts),
            )

    def reset(self, seed):
        # A reset is a run boundary, not a way of erasing outstanding work.
        if self.status()["queued"] or self.status()["active"]:
            raise ValueError("Worker has outstanding local work")
        with self.lock:
            if self.counts["pending_calls"] or self.counts["http_in_flight"]:
                raise ValueError("Worker has outstanding requests or propagation")
            self.generation += 1
            digest = hashlib.sha256(f"{self.name}:{seed}".encode()).digest()
            self.rng.seed(int.from_bytes(digest[:8], "big"))
            self.events.clear()
            self.sequence = 0
            self.counts.clear()

    def local(self, data):
        queued_at = time.monotonic()
        token = object()
        with self.condition:
            self.queue.append(token)
            while self.queue[0] is not token or self.active:
                self.condition.wait()
            self.queue.popleft()
            self.active = True
        started = time.monotonic()
        cpu_started = time.thread_time()
        try:
            with self.lock:
                demand = self.rng.expovariate(
                    1 / self.means[data["request_class"] == "write"]
                )
            accumulator = 1
            deadline = time.thread_time() + demand
            while time.thread_time() < deadline:
                accumulator = (1664525 * accumulator + 1013904223) & 0xFFFFFFFF
            key = int(data["key"]) % 1024
            if data["request_class"] == "write":
                self.db.execute("UPDATE kv SET v=? WHERE k=?", (data["id"], key))
                self.db.commit()
            else:
                self.db.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        finally:
            finished = time.monotonic()
            cpu = time.thread_time() - cpu_started
            with self.condition:
                self.active = False
                self.condition.notify_all()
            self.event(
                "local",
                request_class=data["request_class"],
                request_id=data["id"],
                queue_seconds=started - queued_at,
                local_seconds=finished - started,
                cpu_seconds=cpu,
            )
            with self.lock:
                self.counts["local_completed"] += 1
                self.counts["local_worker_seconds"] += finished - started
                self.counts["local_cpu_seconds"] += cpu

    def child(self, target, data):
        started = time.monotonic()
        success = False
        child_seconds = None
        try:
            request = Request(
                target,
                data=json.dumps(data).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urlopen(request, timeout=30) as response:
                result = json.load(response)
                child_seconds = result["response_seconds"]
                success = response.status == 200
            return success
        except (OSError, ValueError, TimeoutError):
            return False
        finally:
            elapsed = time.monotonic() - started
            self.event(
                "rpc",
                request_class=data["request_class"],
                request_id=data["id"],
                target=target,
                success=success,
                elapsed_seconds=elapsed,
                child_seconds=child_seconds,
                transport_seconds=(
                    max(0, elapsed - child_seconds)
                    if child_seconds is not None
                    else None
                ),
            )
            with self.lock:
                self.counts["pending_calls"] -= 1
                self.counts["child_failures"] += not success

    def work(self, data):
        if data.get("request_class") not in ("read", "write"):
            raise ValueError("Unknown request class")
        if data.get("mode") not in ("async", "quorum1", "all"):
            raise ValueError("Unknown acknowledgement mode")
        started = time.monotonic()
        self.local(data)
        success = True
        if self.targets:
            if data["request_class"] == "read":
                with self.lock:
                    chosen = 0 if self.rng.random() < 0.75 else 1
                selected = (self.targets[chosen],)
            else:
                selected = self.targets
            with self.lock:
                self.counts["pending_calls"] += len(selected)
            futures = [
                self.calls.submit(self.child, target, data) for target in selected
            ]
            required = (
                1
                if data["request_class"] == "read" or data["mode"] == "quorum1"
                else len(futures)
            )
            if data["request_class"] == "read" or data["mode"] != "async":
                returned = 0
                for future in as_completed(futures):
                    returned += bool(future.result())
                    if returned >= required:
                        break
                success = returned >= required
            # Future branches are deliberately neither cancelled nor discarded.
        elapsed = time.monotonic() - started
        self.event(
            "response",
            request_class=data["request_class"],
            request_id=data["id"],
            response_seconds=elapsed,
            success=success,
        )
        return dict(response_seconds=elapsed, success=success, service=self.name)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 512


def handler(worker, *, evidence=False):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def reply(self, status, value):
            raw = json.dumps(value, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.reply(200, {"ready": True})
            elif evidence and parsed.path == "/status":
                self.reply(200, worker.status())
            elif evidence and parsed.path == "/events":
                since = int(parse_qs(parsed.query).get("since", [0])[0])
                with worker.lock:
                    values = [dict(x) for x in worker.events if x["sequence"] > since]
                    lost = bool(
                        worker.events and since < worker.events[0]["sequence"] - 1
                    )
                self.reply(200, {"events": values, "lost": lost})
            else:
                self.reply(404, {"error": "unknown endpoint"})

        def do_POST(self):
            size = int(self.headers.get("Content-Length", 0))
            if not 0 < size <= 8192:
                self.reply(400, {"error": "invalid body size"})
                return
            try:
                data = json.loads(self.rfile.read(size))
                if evidence and self.path == "/reset":
                    worker.reset(int(data["seed"]))
                    self.reply(200, worker.status())
                    return
                if self.path != "/work" or evidence:
                    self.reply(404, {"error": "unknown endpoint"})
                    return
                with worker.lock:
                    worker.counts["http_in_flight"] += 1
                try:
                    result = worker.work(data)
                finally:
                    with worker.lock:
                        worker.counts["http_in_flight"] -= 1
                self.reply(200 if result["success"] else 503, result)
            except Exception as error:
                worker.event("error", message=f"{type(error).__name__}: {error}")
                self.reply(500, {"error": f"{type(error).__name__}: {error}"})

    return Handler


def main():
    worker = Worker(
        os.environ["SERVICE_NAME"],
        os.environ["DATABASE"],
        targets=filter(None, os.environ.get("TARGETS", "").split(",")),
        means=(
            float(os.environ["READ_CPU_SECONDS"]),
            float(os.environ["WRITE_CPU_SECONDS"]),
        ),
    )
    metrics = Server(("0.0.0.0", 9091), handler(worker, evidence=True))
    threading.Thread(target=metrics.serve_forever, daemon=True).start()
    print(json.dumps({"ready": True, "service": worker.name}), flush=True)
    Server(("0.0.0.0", 8080), handler(worker)).serve_forever()


if __name__ == "__main__":
    main()
