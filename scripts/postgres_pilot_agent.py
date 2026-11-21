"""SQL gateway and process observer for an isolated real-PostgreSQL pilot.

Only the gateway imports psycopg2. Observer containers share their pod's PID
namespace, never the host's. No database protocol is simulated here.
"""

import argparse
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import queue
import threading
import time
from urllib.request import urlopen

if __package__:
    from .stateful_pilot_server import Server
else:
    from stateful_pilot_server import Server
from http.server import BaseHTTPRequestHandler

MEMBERS = ("primary", "standby1", "standby2")
READ = "SELECT sum(length(md5(payload))) FROM bench WHERE id BETWEEN %s AND %s"
WRITE = (
    "UPDATE bench SET version=version+1, "
    "payload=md5(version::text)||substring(payload FROM 33) WHERE id=%s"
)


def processes():
    result = []
    ticks = os.sysconf("SC_CLK_TCK")
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            if (path / "comm").read_text().strip() != "postgres":
                continue
            command = (
                (path / "cmdline")
                .read_bytes()
                .replace(b"\0", b" ")
                .decode(errors="replace")
            )
            fields = (path / "stat").read_text().rsplit(")", 1)[1].split()
            result.append(
                dict(
                    pid=int(path.name),
                    command=command,
                    cpu_seconds=(int(fields[11]) + int(fields[12])) / ticks,
                    start_ticks=int(fields[19]),
                )
            )
        except (OSError, ValueError, IndexError):
            continue
    snapshot = dict(epoch=time.time(), processes=result)
    counter = os.environ.get("PGPILOT_AGGREGATE_PATH")
    if counter:
        try:
            epoch, usage = Path(counter).read_text().strip().split(",")
            snapshot["aggregate_container_cpu"] = {
                "epoch": float(epoch), "usage_seconds": int(usage) / 1_000_000,
            }
        except (OSError, ValueError):
            pass
    return snapshot


class Pool:
    def __init__(self, host, role, password, *, size=16):
        import psycopg2

        self.idle = queue.Queue()
        self.pids = []
        self.host = host
        for _ in range(size):
            connection = psycopg2.connect(
                host=host,
                user="postgres",
                password=password,
                dbname="pilot",
                connect_timeout=10,
                application_name=f"pgp1_{role}",
            )
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='15s'")
                cursor.execute("SELECT pg_backend_pid()")
                self.pids.append(cursor.fetchone()[0])
            self.idle.put(connection)

    def execute(self, sql, params=None):
        connection = self.idle.get(timeout=20)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall() if cursor.description else []
        finally:
            self.idle.put(connection)

    def configure(self, mode):
        connections = [self.idle.get(timeout=20) for _ in self.pids]
        try:
            for connection in connections:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SET synchronous_commit TO "
                        + ("local" if mode == "async" else "remote_apply")
                    )
        finally:
            for connection in connections:
                self.idle.put(connection)


class Gateway:
    def __init__(self):
        layout = os.environ.get("PGPILOT_READ_LAYOUT")
        if layout not in (None, "one", "two"):
            raise ValueError("Unknown read layout")
        self.members = MEMBERS[:2] if layout == "one" else MEMBERS
        self.read_layout = layout
        password = os.environ["PGPASSWORD"]
        self.write = Pool("primary", "write", password)
        self.reads = {n: Pool(n, "read", password) for n in self.members[1:]}
        self.admin = {n: Pool(n, "monitor", password, size=2) for n in self.members}
        self.events = deque(maxlen=300_000)
        self.sequence = 0
        self.mode = None
        self.counts = Counter()
        self.lock = threading.Lock()

    def event(self, kind, **data):
        with self.lock:
            self.sequence += 1
            self.events.append(
                dict(type=kind, sequence=self.sequence, epoch=time.time(), **data)
            )

    def configure(self, mode):
        if mode not in ("async", "quorum1", "all"):
            raise ValueError("Unknown mode")
        with self.lock:
            if self.counts["in_flight"]:
                raise ValueError("Cannot switch acknowledgement during active traffic")
        if self.read_layout and mode != "async":
            raise ValueError("The read-layout pilot requires local commits")
        setting = (
            ""
            if mode == "async"
            else f"ANY {1 if mode=='quorum1' else 2} (standby1, standby2)"
        )
        if self.read_layout is None:
            self.admin["primary"].execute(
                "ALTER SYSTEM SET synchronous_standby_names=%s", (setting,)
            )
            self.admin["primary"].execute("SELECT pg_reload_conf()")
            time.sleep(1)
            current = self.admin["primary"].execute("SHOW synchronous_standby_names")[0][0]
            if current != setting:
                raise ValueError("PostgreSQL replication configuration did not take effect")
        self.write.configure(mode)
        self.mode = mode
        with self.lock:
            self.events.clear()
            self.sequence = 0
            self.counts.clear()
        return self.snapshot()

    def work(self, data):
        if data.get("mode") != self.mode:
            raise ValueError(
                "Request mode differs from frozen PostgreSQL configuration"
            )
        c = data["request_class"]
        if c not in ("read", "write"):
            raise ValueError("Invalid request class")
        with self.lock:
            self.counts["in_flight"] += 1
        begin = time.monotonic()
        try:
            if c == "read":
                parity = int(data["key"]) % (2 if self.read_layout else 4)
                member = "standby1" if self.read_layout == "one" or parity else "standby2"
                key = int(data["key"]) % 95_000 + 1
                self.reads[member].execute(READ, (key, key + 4095))
            else:
                member = "primary"
                self.write.execute(WRITE, (int(data["key"]) % 100_000 + 1,))
            elapsed = time.monotonic() - begin
            self.event(
                "response",
                request_class=c,
                request_id=data["id"],
                member=member,
                response_seconds=elapsed,
            )
            with self.lock:
                self.counts[c] += 1
            return dict(success=True, response_seconds=elapsed, member=member)
        finally:
            with self.lock:
                self.counts["in_flight"] -= 1

    def snapshot(self):
        snapshots = {}
        for member in self.members:
            with urlopen(f"http://{member}:9092/status", timeout=5) as response:
                snapshots[member] = json.load(response)
        primary = self.admin["primary"].execute(
            "SELECT application_name,state,sync_state,"
            "pg_wal_lsn_diff(pg_current_wal_lsn(),replay_lsn)::float8,"
            "extract(epoch FROM write_lag)::float8,extract(epoch FROM flush_lag)::float8,"
            "extract(epoch FROM replay_lag)::float8 FROM pg_stat_replication ORDER BY application_name"
        )
        waits = {
            n: self.admin[n].execute(
                "SELECT application_name,coalesce(wait_event_type,''),"
                "coalesce(wait_event,''),count(*) FROM pg_stat_activity WHERE application_name LIKE 'pgp1_%' "
                "GROUP BY 1,2,3"
            )
            for n in self.members
        }
        return dict(
            epoch=time.time(),
            members=snapshots,
            replication=primary,
            waits=waits,
            pids={
                "primary": self.write.pids,
                **{n: p.pids for n, p in self.reads.items()},
            },
            mode=self.mode,
            read_layout=self.read_layout,
            counters=dict(self.counts),
        )

    def witness(self, count):
        if self.mode != "async":
            raise ValueError(
                "Isolated replay witnesses require local-commit characterization"
            )
        samples, probes = [], {n: [] for n in self.members}
        for member in self.members:
            for _ in range(30):
                start = time.monotonic()
                self.admin[member].execute("SELECT 1")
                probes[member].append(time.monotonic() - start)
        with ThreadPoolExecutor(max_workers=2) as pool:
            for i in range(count):
                started = time.monotonic()
                self.write.execute(WRITE, (i % 100_000 + 1,))
                local = time.monotonic() - started
                marker = self.admin["primary"].execute(
                    "SELECT pg_current_wal_flush_lsn()::text"
                )[0][0]

                def wait(member):
                    checks = 0
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        checks += 1
                        if self.admin[member].execute(
                            "SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (marker,)
                        )[0][0]:
                            return dict(
                                seconds_since_local_commit=max(
                                    0, time.monotonic() - started - local
                                ),
                                poll_count=checks,
                            )
                        time.sleep(0.001)
                    raise TimeoutError("Replay witness exceeded 15 seconds")

                pending = {n: pool.submit(wait, n) for n in self.members[1:]}
                samples.append(
                    dict(
                        local_commit_seconds=local,
                        standbys={n: p.result() for n, p in pending.items()},
                    )
                )
                time.sleep(0.03)
        return dict(
            samples=samples,
            query_round_trip_seconds=probes,
            scope="upper proxy: post-commit marker query, polling and transport included",
        )


def handler(gateway=None):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def reply(self, status, value):
            raw = json.dumps(value, default=str, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Content-Type", "application/json")
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            try:
                if self.path == "/health":
                    self.reply(200, {"ready": True})
                elif gateway is None:
                    self.reply(200, processes())
                elif self.path == "/snapshot":
                    self.reply(200, gateway.snapshot())
                elif self.path == "/events":
                    with gateway.lock:
                        self.reply(
                            200,
                            {
                                "events": list(gateway.events),
                                "lost": bool(
                                    gateway.events
                                    and gateway.events[0]["sequence"] != 1
                                ),
                            },
                        )
                else:
                    self.reply(404, {"error": "unknown endpoint"})
            except Exception as error:
                self.reply(500, {"error": f"{type(error).__name__}: {error}"})

        def do_POST(self):
            try:
                size = int(self.headers.get("Content-Length", 0))
                if not 0 < size < 8192 or gateway is None:
                    raise ValueError("Invalid request")
                data = json.loads(self.rfile.read(size))
                if self.path == "/work":
                    result = gateway.work(data)
                elif self.path == "/configure":
                    result = gateway.configure(data["mode"])
                elif self.path == "/witness":
                    result = gateway.witness(min(1000, int(data["count"])))
                else:
                    raise ValueError("Unknown endpoint")
                self.reply(200, result)
            except Exception as error:
                self.reply(500, {"error": f"{type(error).__name__}: {error}"})

    return Handler


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--observer", action="store_true")
    args = parser.parse_args()
    if args.observer:
        Server(("0.0.0.0", 9092), handler()).serve_forever()
    else:
        gateway = Gateway()
        server = Server(("0.0.0.0", 9091), handler(gateway))
        threading.Thread(target=server.serve_forever, daemon=True).start()
        Server(("0.0.0.0", 8080), handler(gateway)).serve_forever()
