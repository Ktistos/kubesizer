"""Real-thread HTTP mechanism fixture for cmp1, not a product emulator.

Local occupied-worker and delay phases are explicit optional span evidence.
The paced link is an application byte scheduler, not a physical TCP model.
"""

from collections import Counter, OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import random
import threading
import time


FAMILIES = ("broker", "pool", "cache", "link", "external", "serverless")
KEY_PROFILE = [f"key-{k}" for k in range(16) for _ in range(16 - k)]


class Rejected(Exception):
    pass


class Gate:
    """Bounded FIFO admission implemented with real condition variables."""

    def __init__(self, capacity, pending=None, timeout=0):
        self.capacity, self.pending, self.timeout = capacity, pending, timeout
        self.condition, self.queue, self.active = threading.Condition(), deque(), 0

    @contextmanager
    def enter(self):
        token, beginning = object(), time.monotonic()
        with self.condition:
            if self.active >= self.capacity or self.queue:
                if self.pending is not None and len(self.queue) >= self.pending:
                    raise Rejected("queue_full")
                self.queue.append(token)
                while self.queue[0] is not token or self.active >= self.capacity:
                    remaining = (
                        self.timeout - (time.monotonic() - beginning)
                        if self.timeout
                        else None
                    )
                    if remaining is not None and remaining <= 0:
                        self.queue.remove(token)
                        self.condition.notify_all()
                        raise Rejected("queue_timeout")
                    self.condition.wait(remaining)
                self.queue.popleft()
            self.active += 1
        try:
            yield time.monotonic() - beginning
        finally:
            with self.condition:
                self.active -= 1
                self.condition.notify_all()

    def status(self):
        with self.condition:
            return dict(
                active=self.active, queued=len(self.queue), capacity=self.capacity
            )


class PacedLink:
    """One physical sender thread with round-robin 4 KiB application chunks."""

    def __init__(self, rate, record):
        self.rate, self.record = rate, record
        self.queue, self.condition, self.stopping = (
            deque(),
            threading.Condition(),
            False,
        )
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.outstanding, self.bytes_sent, self.busy_seconds = 0, 0, 0.0
        self.thread.start()

    def transfer(self, size, request_id):
        item = dict(
            remaining=size, work=0.0, done=threading.Event(), size=size, id=request_id
        )
        with self.condition:
            self.outstanding += 1
            self.queue.append(item)
            self.condition.notify()
        if not item["done"].wait(20):
            raise Rejected("link_timeout")
        self.record("link_work", request_id, item["work"], bytes=size)

    def run(self):
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.queue or self.stopping)
                if self.stopping and not self.queue:
                    return
                item = self.queue.popleft()
            chunk = min(4096, item["remaining"])
            start = time.monotonic()
            time.sleep(chunk / self.rate)
            elapsed = time.monotonic() - start
            item["work"] += elapsed
            with self.condition:
                self.bytes_sent += chunk
                self.busy_seconds += elapsed
            item["remaining"] -= chunk
            if item["remaining"]:
                with self.condition:
                    self.queue.append(item)
            else:
                with self.condition:
                    self.outstanding -= 1
                item["done"].set()

    def status(self):
        with self.condition:
            return dict(
                outstanding_transfers=self.outstanding,
                bytes_sent=self.bytes_sent,
                occupied_sender_seconds=self.busy_seconds,
            )

    def close(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
        self.thread.join(timeout=25)
        if self.thread.is_alive():
            raise RuntimeError("Paced link has outstanding work")


class Engine:
    def __init__(self):
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=256)
        self.link, self.in_flight, self.background = None, 0, 0
        self.family, self.capacity = None, 1
        self.reset("pool", 1, 1)

    def record(self, phase, request_id, seconds, **extra):
        with self.lock:
            self.sequence += 1
            self.events.append(
                dict(
                    sequence=self.sequence,
                    epoch=time.time(),
                    phase=phase,
                    request_id=request_id,
                    seconds=seconds,
                    **extra,
                )
            )
            self.counts[phase + "_count"] += 1
            self.counts[phase + "_seconds"] += seconds

    def sleep(self, phase, request_id, mean):
        with self.lock:
            duration = self.rng.expovariate(1 / mean)
        start = time.monotonic()
        time.sleep(duration)
        elapsed = time.monotonic() - start
        self.record(phase, request_id, elapsed)
        return elapsed

    def reset(self, family, capacity, seed):
        if family not in FAMILIES or capacity not in (1, 2):
            raise ValueError("Unknown cmp1 family or capacity")
        with self.lock:
            if self.in_flight or self.background:
                raise ValueError("Cannot reset while requests or old attempts remain")
        if self.link:
            self.link.close()
        with self.lock:
            self.family, self.capacity = family, capacity
            self.rng = random.Random(seed)
            self.counts, self.events, self.sequence = (
                Counter(),
                deque(maxlen=200_000),
                0,
            )
            self.worker_admission = [Gate(4, 8, 0.2) for _ in range(capacity)]
            self.worker_processing = [Gate(1) for _ in range(capacity)]
            self.pool = Gate(2 * capacity, 8, 0.2)
            self.backend = Gate(4)
            self.api = Gate(4, 8, 0.2)
            self.instance_gates = [Gate(2, 8, 0.2) for _ in range(capacity)]
            self.instance_conditions = [threading.Condition() for _ in range(capacity)]
            self.instance_ready = [False] * capacity
            self.instance_starting = [False] * capacity
            self.instance_active = [0] * capacity
            self.instance_last = [time.monotonic()] * capacity
            self.cache, self.cache_bytes = OrderedDict(), 0
            self.tokens, self.token_update = 2.0, time.monotonic()
            self.next_worker = 0
            self.link = (
                PacedLink(1024 * 1024 * capacity, self.record)
                if family == "link"
                else None
            )

    def status(self):
        with self.lock:
            return dict(
                family=self.family,
                capacity=self.capacity,
                in_flight=self.in_flight,
                background=self.background,
                sequence=self.sequence,
                evidence_lost=self.sequence > len(self.events),
                counters=dict(self.counts),
                pool=self.pool.status(),
                backend=self.backend.status(),
                workers=[g.status() for g in self.worker_admission],
                instances=[g.status() for g in self.instance_gates],
                link=self.link.status() if self.link else None,
                process_cpu_seconds=time.process_time(),
            )

    def spawn(self, function, *args):
        with self.lock:
            self.background += 1

        def invoke():
            try:
                return function(*args)
            finally:
                with self.lock:
                    self.background -= 1

        return self.executor.submit(invoke)

    def broker(self, request_id, worker):
        with self.worker_admission[worker].enter() as queued:
            self.record("worker_admission", request_id, queued)
            with self.worker_processing[worker].enter():
                self.sleep("worker_work", request_id, 0.02)
            self.sleep("worker_block", request_id, 0.01)

    def lookup(self, request_id, key):
        start = time.monotonic()
        with self.lock:
            expiry = self.cache.get(key)
            hit = expiry is not None and expiry > time.monotonic()
            if hit:
                self.cache.move_to_end(key)
            self.counts["cache_hits" if hit else "cache_misses"] += 1
        self.record(
            "cache_lookup",
            request_id,
            max(time.monotonic() - start, 1e-6),
            hit=hit,
            key=key,
        )
        if not hit:
            with self.backend.enter():
                self.sleep("cache_backend", request_id, 0.03)
            with self.lock:
                now = time.monotonic()
                for old in list(self.cache):
                    if self.cache[old] <= now:
                        del self.cache[old]
                self.cache.pop(key, None)
                while len(self.cache) >= 4 * self.capacity:
                    self.cache.popitem(last=False)
                self.cache[key] = now + 1

    def api_attempt(self, request_id):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(
                2, self.tokens + (now - self.token_update) * 20 * self.capacity
            )
            self.token_update = now
            if self.tokens < 1:
                self.counts["quota_rejections"] += 1
                raise Rejected("quota")
            self.tokens -= 1
            self.counts["attempts"] += 1
        with self.api.enter():
            self.sleep("api_delay", request_id, 0.02)
            with self.lock:
                failed = self.rng.random() < 0.1
                self.counts["attempt_failures"] += int(failed)
            if failed:
                raise Rejected("declared_attempt_failure")

    def serverless(self, request_id, instance):
        with self.instance_gates[instance].enter():
            condition = self.instance_conditions[instance]
            with condition:
                if (
                    not self.instance_active[instance]
                    and time.monotonic() - self.instance_last[instance] >= 0.2
                ):
                    self.instance_ready[instance] = False
                self.instance_active[instance] += 1
                cold = (
                    not self.instance_ready[instance]
                    and not self.instance_starting[instance]
                )
                if cold:
                    self.instance_starting[instance] = True
            try:
                if cold:
                    start = time.monotonic()
                    time.sleep(0.15)
                    self.record("startup", request_id, time.monotonic() - start)
                    with condition:
                        self.instance_ready[instance] = True
                        self.instance_starting[instance] = False
                        condition.notify_all()
                with condition:
                    condition.wait_for(lambda: self.instance_ready[instance])
                self.sleep("function_delay", request_id, 0.02)
            finally:
                with condition:
                    self.instance_active[instance] -= 1
                    self.instance_last[instance] = time.monotonic()

    def work(self, data):
        start, request_id = time.monotonic(), str(data["id"])
        with self.lock:
            self.in_flight += 1
            worker = self.next_worker % self.capacity
            if self.family != "broker":
                self.next_worker += 1
        success, error = True, None
        try:
            if self.family == "broker":
                self.sleep("publish", request_id, 0.001)
                with self.lock:
                    worker = self.next_worker % self.capacity
                    self.next_worker += 1
                job = self.spawn(self.broker, request_id, worker)
                ack = time.monotonic() - start
                job.result(timeout=20)
            elif self.family == "pool":
                with self.pool.enter():
                    self.sleep("pool_hold", request_id, 0.04)
            elif self.family == "cache":
                self.lookup(request_id, str(data["key"]))
            elif self.family == "link":
                self.link.transfer(int(data["size_bytes"]), request_id)
            elif self.family == "external":
                for attempt in range(3):
                    future = self.spawn(self.api_attempt, request_id)
                    try:
                        future.result(timeout=0.06)
                        break
                    except (Rejected, FutureTimeout):
                        if attempt == 2:
                            raise Rejected("attempts_exhausted")
                        time.sleep(0.005)
            else:
                self.serverless(request_id, worker)
        except (Rejected, FutureTimeout) as failure:
            success, error = False, str(failure)
        finally:
            elapsed = time.monotonic() - start
            self.record("response", request_id, elapsed, success=success, error=error)
            with self.lock:
                self.in_flight -= 1
                self.counts["completed"] += 1
                self.counts["failed"] += int(not success)
        result = dict(success=success, error=error, response_seconds=elapsed)
        if self.family == "broker":
            result["publisher_ack_seconds"] = ack
        return result

    def close(self):
        self.executor.shutdown(wait=True)
        if self.link:
            self.link.close()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


def handler(engine, *, evidence=False):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, value):
            raw = json.dumps(value).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            if self.path == "/health":
                self.reply(200, dict(ready=True))
            elif evidence and self.path == "/status":
                self.reply(200, engine.status())
            elif evidence and self.path == "/events":
                with engine.lock:
                    self.reply(
                        200,
                        dict(
                            events=list(engine.events),
                            lost=engine.sequence > len(engine.events),
                        ),
                    )
            else:
                self.reply(404, dict(error="unknown endpoint"))

        def do_POST(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 < length <= 8192:
                    raise ValueError("invalid body length")
                data = json.loads(self.rfile.read(length))
                if evidence and self.path == "/reset":
                    engine.reset(
                        data["family"], int(data["capacity"]), int(data["seed"])
                    )
                    self.reply(200, engine.status())
                elif not evidence and self.path == "/work":
                    if data.get("key") not in KEY_PROFILE or data.get(
                        "size_bytes"
                    ) not in (65536, 262144):
                        raise ValueError("Invalid request key/payload declaration")
                    self.reply(200, engine.work(data))
                else:
                    self.reply(404, dict(error="unknown endpoint"))
            except (ValueError, KeyError, TypeError) as error:
                self.reply(400, dict(error=str(error)))

    return Handler


if __name__ == "__main__":
    engine = Engine()
    observer = Server(("0.0.0.0", 9091), handler(engine, evidence=True))
    threading.Thread(target=observer.serve_forever, daemon=True).start()
    Server(("0.0.0.0", 8080), handler(engine)).serve_forever()
