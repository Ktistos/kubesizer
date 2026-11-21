"""Pod-local MinIO CPU/I/O counters; no host PID namespace or object access."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import time


def snapshot():
    values = []
    for path in Path("/proc").iterdir():
        if not path.name.isdigit():
            continue
        try:
            if (path / "comm").read_text().strip() != "minio":
                continue
            stat = (path / "stat").read_text().rsplit(")", 1)[1].split()
            value = dict(
                pid=int(path.name),
                start_ticks=int(stat[19]),
                cpu_seconds=(int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
            )
            try:
                value["io"] = {
                    k: int(v)
                    for line in (path / "io").read_text().splitlines()
                    for k, v in [line.split(":", 1)]
                }
            except OSError as error:
                value["io_unavailable"] = type(error).__name__
            values.append(value)
        except (OSError, ValueError, IndexError):
            continue
    return dict(epoch=time.time(), processes=values)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        raw = json.dumps(snapshot()).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9092), Handler).serve_forever()
