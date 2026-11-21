#!/usr/bin/env python3
"""Apply the test-only fragment timing probes to the pinned MinIO source tree."""

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "experiments/stateful_pilot/minio_fragment_timing.go"


def replace_once(source, before, after):
    if source.count(before) != 1:
        raise ValueError(f"Expected one pinned source anchor {before!r}")
    return source.replace(before, after, 1)


def instrument(source):
    command = source / "cmd"
    write_path = command / "xl-storage.go"
    send_path = command / "storage-rest-client.go"
    write = write_path.read_text()
    send = send_path.read_text()
    anchor = "\treturn s.writeAllDirect(ctx, filePath, fileSize, r, os.O_CREATE|os.O_WRONLY|os.O_EXCL, volumeDir, false)"
    write = replace_once(
        write,
        anchor,
        """\tif timing := newKubesizerTiming(origvolume, volume, path, "receive", fileSize); timing != nil {
        defer func() { timing.finish(err) }()
        ctx = context.WithValue(ctx, kubesizerFragmentKey{}, timing)
    }
"""
        + anchor,
    )
    before = """\tvar written int64
\tif odirectEnabled {
\t\twritten, err = xioutil.CopyAligned(diskHealthWriter(ctx, w), r, *bufp, fileSize, w)
\t} else {
\t\twritten, err = io.CopyBuffer(diskHealthWriter(ctx, w), r, *bufp)
\t}"""
    after = """\ttiming, _ := ctx.Value(kubesizerFragmentKey{}).(*kubesizerFragmentTiming)
    writer := diskHealthWriter(ctx, w)
    if timing != nil {
        timing.DirectIO = odirectEnabled
        r = kubesizerTimedReader{reader: r, timing: timing}
        writer = kubesizerTimedWriter{writer: writer, timing: timing}
    }
    var written int64
    if odirectEnabled {
        written, err = xioutil.CopyAligned(writer, r, *bufp, fileSize, w)
    } else {
        written, err = io.CopyBuffer(writer, r, *bufp)
    }"""
    write = replace_once(write, before, after)
    write = replace_once(
        write,
        "\tif err = Fdatasync(w); err != nil {",
        "\tif err = kubesizerTimedSync(w, timing); err != nil {",
    )
    before = "func (client *storageRESTClient) CreateFile(ctx context.Context, origvolume, volume, path string, size int64, reader io.Reader) error {"
    after = (
        before.removesuffix("error {")
        + """(err error) {
    if timing := newKubesizerTiming(origvolume, volume, path, "send", size); timing != nil {
        timing.Peer = client.endpoint.Host
        defer func() { timing.finish(err) }()
        reader = kubesizerTimedReader{reader: reader, timing: timing}
    }"""
    )
    send = replace_once(send, before, after)
    write_path.write_text(write)
    send_path.write_text(send)
    (command / "kubesizer_fragment_timing.go").write_bytes(PROBE.read_bytes())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    instrument(parser.parse_args().source)
