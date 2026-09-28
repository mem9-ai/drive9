"""Clean immediate-vs-drained atomic replacement diagnosis; single existing mount."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

BASE = Path(__file__).resolve().parent
sys.path.insert(0, '/home/ubuntu/drive9-none-b-memory-20260913T0440')
import repro_none_b_memory as h
sys.path.insert(0, '/home/ubuntu/drive9-sixway-issue917-20260911')
from cases import payload, write_file


def main():
    h.BASE = BASE; h.worker_pid = 354225
    results = []
    with Path('/home/ubuntu/drive9-sixway-20260911/coordinator.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert hashlib.sha256(Path('/proc/354225/exe').read_bytes()).hexdigest() == h.SHA
        assert h.memory()['host']['MemAvailable'] > 4*1024**3
        watcher = threading.Thread(target=h.watcher, daemon=True); watcher.start()
        data = [payload(4096, i) for i in range(21)]
        try:
            for rep in range(3):
                for mode in (['immediate', 'drained'] if rep != 1 else ['drained', 'immediate']):
                    label = f'{mode}-{rep}'
                    h.stage = label
                    root = h.MOUNT / BASE.name / label
                    root.mkdir(parents=True, exist_ok=False)
                    target = root / 'target'
                    write_file(target, data[0]); h.drain()
                    rows = []
                    start = time.time()
                    for i in range(20):
                        assert not h.stop.is_set(), 'Memory safety stop'
                        staged = root / f'staged-{i}'
                        t = time.monotonic_ns(); write_file(staged, data[i+1]); write_s = (time.monotonic_ns()-t)/1e9
                        drain_s = 0
                        if mode == 'drained':
                            t = time.monotonic_ns(); h.drain(); drain_s = (time.monotonic_ns()-t)/1e9
                        t = time.monotonic_ns(); os.replace(staged, target); replace_s = (time.monotonic_ns()-t)/1e9
                        rows.append({'write_s': write_s, 'drain_s': drain_s, 'replace_s': replace_s})
                    end = time.time()
                    h.drain()
                    assert target.read_bytes() == data[-1] and len(list(root.iterdir())) == 1
                    result = {'label': label, 'mode': mode, 'repeat': rep, 'verified': True, 'operations': 20, 'start_epoch': start, 'end_epoch': end, 'rows': rows}
                    result['phase_s'] = {k: sum(r[k] for r in rows) for k in ('write_s', 'drain_s', 'replace_s')}
                    h.save(label+'.json', result); results.append(result)
                    print(json.dumps({'label': label, **result['phase_s']}), flush=True)
            h.save('summary.json', {'state': 'completed', 'results': results, 'max_rss_mib': max(r.get('VmRSS',0) for r in h.samples)/1024**2, 'min_available_mib': min(r['host']['MemAvailable'] for r in h.samples)/1024**2})
        finally:
            h.done.set(); watcher.join(timeout=3)


if __name__ == '__main__': main()
