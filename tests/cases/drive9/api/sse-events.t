#!/bin/sh
# drive9/api/sse-events: SSE /v1/events initial sync, live file_changed delivery,
# structural reset, cursor replay, and >1000-event paginated backlog replay (--no-mount).
set -eu

SSE_BACKLOG_COUNT="${SSE_BACKLOG_COUNT:-1050}"
SSE_ROOT="/sse-contract-$$-$(date +%s)"
HELPER="$RUN_DIR/sse_helper.py"
export SSE_BACKLOG_COUNT SSE_ROOT HELPER

cat > "$HELPER" <<'PYEOF'
import concurrent.futures
import http.client
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request

SERVER = os.environ["SERVER"].rstrip("/")
API_KEY = os.environ["API_KEY"]
SSE_ROOT = os.environ["SSE_ROOT"]
BACKLOG = int(os.environ.get("SSE_BACKLOG_COUNT", "1050"))
STATE_FILE = os.path.join(os.environ["RUN_DIR"], "sse_state.json")


def http_req(method, path, body=b"", headers=None):
    hdrs = {"Authorization": f"Bearer {API_KEY}"}
    if headers:
        hdrs.update(headers)
    r = urllib.request.Request(
        SERVER + path,
        data=body if method in ("POST", "PUT") else None,
        headers=hdrs,
        method=method,
    )
    with urllib.request.urlopen(r, timeout=30) as resp:
        return resp.status, resp.read()


def read_sse_until(since, stop_pred, timeout_s=45.0):
    parsed = urllib.parse.urlparse(SERVER)
    conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parsed.hostname, parsed.port, timeout=timeout_s)
    conn.request("GET", f"/v1/events?since={since}", headers={"Authorization": f"Bearer {API_KEY}"})
    resp = conn.getresponse()
    assert resp.status == 200, f"GET /v1/events?since={since} got {resp.status}"
    events = []
    cur_event = None
    deadline = time.time() + timeout_s
    try:
        while time.time() < deadline:
            raw = resp.fp.readline()
            if not raw:
                break
            line = raw.decode("utf-8", "replace").rstrip("\r\n")
            if line.startswith("event: "):
                cur_event = line[len("event: "):].strip()
            elif line.startswith("data: "):
                data = json.loads(line[len("data: "):])
                events.append((cur_event, data))
                if stop_pred(events):
                    break
    finally:
        conn.close()
    return events


mode = sys.argv[1]
if mode == "initial-sync":
    st, _ = http_req("POST", f"/v1/fs{SSE_ROOT}?mkdir=1")
    assert st == 200
    st, _ = http_req("POST", f"/v1/fs{SSE_ROOT}/backlog?mkdir=1")
    assert st == 200

    evs = read_sse_until(0, lambda es: any(e == "reset" for e, _ in es) and any(e == "heartbeat" for e, _ in es), 15.0)
    resets = [d for e, d in evs if e == "reset"]
    hbs = [d for e, d in evs if e == "heartbeat"]
    assert len(resets) == 1 and resets[0].get("reason") == "initial_sync", evs
    assert len(hbs) >= 1, evs

elif mode == "live-and-replay":
    captured = []
    ready = threading.Event()

    def subscriber():
        parsed = urllib.parse.urlparse(SERVER)
        conn_cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        conn = conn_cls(parsed.hostname, parsed.port, timeout=20)
        conn.request("GET", "/v1/events?since=0", headers={"Authorization": f"Bearer {API_KEY}"})
        resp = conn.getresponse()
        cur_ev = None
        try:
            while True:
                line = resp.fp.readline().decode("utf-8", "replace").rstrip("\r\n")
                if line.startswith("event: "):
                    cur_ev = line[7:].strip()
                elif line.startswith("data: "):
                    d = json.loads(line[6:])
                    captured.append((cur_ev, d))
                    if cur_ev == "heartbeat":
                        ready.set()
                    fc = [x for e, x in captured if e == "file_changed"]
                    if len(fc) >= 2:
                        break
        finally:
            conn.close()

    t = threading.Thread(target=subscriber, daemon=True)
    t.start()
    assert ready.wait(10.0), "subscriber did not receive initial heartbeat"

    http_req("PUT", f"/v1/fs{SSE_ROOT}/seed.txt", b"seed-v1", headers={"X-Dat9-Actor": "sse-actor-1"})
    http_req("PUT", f"/v1/fs{SSE_ROOT}/seed.txt", b"seed-v2", headers={"X-Dat9-Actor": "sse-actor-2"})
    t.join(15.0)

    fc = [d for e, d in captured if e == "file_changed"]
    assert len(fc) >= 2, captured
    s1, s2 = int(fc[0]["seq"]), int(fc[1]["seq"])
    assert s2 > s1, (s1, s2)
    assert fc[0].get("actor") == "sse-actor-1", fc[0]

    # Replay since S1 -> must deliver S2 + heartbeat without seq_too_old/server_restart
    rep = read_sse_until(
        s1,
        lambda es: any(e == "file_changed" and int(d["seq"]) >= s2 for e, d in es) and any(e == "heartbeat" for e, _ in es),
        20.0,
    )
    reasons = [d.get("reason") for e, d in rep if e == "reset"]
    assert "seq_too_old" not in reasons and "server_restart" not in reasons, rep
    rep_fc = [d for e, d in rep if e == "file_changed"]
    assert int(rep_fc[-1]["seq"]) == s2, rep_fc

    with open(STATE_FILE, "w") as f:
        json.dump({"s1": s1, "s2": s2}, f)

elif mode == "backlog-1050":
    with open(STATE_FILE) as f:
        s2 = int(json.load(f)["s2"])

    def write_one(i):
        st, _ = http_req("PUT", f"/v1/fs{SSE_ROOT}/backlog/file-{i}.txt", b"x")
        return st

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        codes = list(pool.map(write_one, range(1, BACKLOG + 1)))
    assert all(c == 200 for c in codes), f"non-200 in backlog writes: {set(codes)}"

    rep = read_sse_until(
        s2,
        lambda es: sum(1 for e, _ in es if e == "file_changed") >= BACKLOG and any(e == "heartbeat" for e, _ in es),
        60.0,
    )
    reasons = [d.get("reason") for e, d in rep if e == "reset"]
    assert "seq_too_old" not in reasons and "server_restart" not in reasons, reasons
    fc_count = sum(1 for e, _ in rep if e == "file_changed")
    assert fc_count >= BACKLOG, f"expected >= {BACKLOG} replayed events, got {fc_count}"

    http_req("DELETE", f"/v1/fs{SSE_ROOT}?recursive=1")

print(f"ok: {mode}")
PYEOF

check shell --req 'drive9-sse-initial-sync' --claim 'sse-initial-sync-reset-and-heartbeat' -- python3 "$HELPER" initial-sync
check shell --req 'drive9-sse-live-and-replay' --claim 'sse-live-delivery-and-cursor-replay' -- python3 "$HELPER" live-and-replay
check shell --req 'drive9-sse-backlog-pagination' --claim 'sse-backlog-over-1000-replay' -- python3 "$HELPER" backlog-1050
