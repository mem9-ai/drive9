"""Repackage the recorded benchmark; never change cases.py or silently reuse results."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
SHA = '95dc072b10f6605f1ffdc95d2bf2de230fadd79a3813d2dca9e1107915144b22'
CASE_SHA = 'b2a577758aaa8c63c73438ebcf96679f39a4673c3958c521e9944dbb9b48b475'
LEGACY = 'sixway-issue917-20260911'


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def prepare(source, output):
    output = output.resolve()
    run_id = output.name
    assert re.fullmatch(r'replay-[a-z0-9][a-z0-9-]{3,70}', run_id), 'Use replay-<unique-id>'
    assert LEGACY not in run_id and not output.exists(), 'Do not reuse a run directory'
    files = ['cases.py', 'run_matrix.py', 'atomic_probe.py', 'summarize.py']
    assert digest(source / 'cases.py') == CASE_SHA
    manifest = json.loads((source.parent / 'MANIFEST.json').read_text())
    for name in files:
        assert digest(source / name) == manifest['original/' + name]['sha256']
    output.mkdir(parents=True, exist_ok=False)
    changes = {}
    for name in files:
        body = (source / name).read_bytes()
        if name in ('run_matrix.py', 'atomic_probe.py'):
            assert body.count(LEGACY.encode()) == 2, 'Unexpected source: inspect before adapting'
            body = body.replace(LEGACY.encode(), run_id.encode())
        (output / name).write_bytes(body)
        changes[name] = {'original_sha256': digest(source / name), 'replay_sha256': digest(output / name)}
    assert digest(output / 'cases.py') == CASE_SHA
    shutil.copyfile(HERE / 'replay.py', output / 'replay.py')
    (output / 'replay-provenance.json').write_text(json.dumps({'run_id': run_id, 'changes': changes, 'only_change': 'data namespace in run_matrix.py and atomic_probe.py; workload unchanged'}, indent=2))
    print(str(output))


def option(args, name):
    for i, value in enumerate(args):
        if value == name and i + 1 < len(args): return args[i + 1]
        if value.startswith(name + '='): return value.split('=', 1)[1]
    return None


def memory(pids):
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    available = int(values['MemAvailable'].split()[0]) * 1024
    rss = {}
    for pid in pids:
        status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines())
        rss[str(pid)] = int(status['VmRSS'].split()[0]) * 1024
    return {'epoch': time.time(), 'available_bytes': available, 'worker_rss_bytes': rss}


def check(allow_profiling=False):
    assert sys.platform == 'linux' and os.getuid() == 1000, 'Run as ubuntu UID 1000 on the specified EC2'
    assert sys.version_info[:3] == (3, 14, 4), 'Recorded Python was 3.14.4; do not hide interpreter changes'
    assert digest(HERE / 'cases.py') == CASE_SHA
    import run_matrix as runner
    assert digest(runner.BIN) == SHA
    subprocess.run(['sudo', '-n', '-u', 'nobody', 'true'], check=True, timeout=10)
    processes = []
    for p in Path('/proc').iterdir():
        if not p.name.isdigit(): continue
        try:
            args = (p / 'cmdline').read_bytes().decode().strip('\0').split('\0')
            if len(args) > 2 and Path(args[0]).name == 'drive9' and args[1] == 'mount' and '--foreground' in args:
                env = dict(v.split('=', 1) for v in (p / 'environ').read_bytes().decode().split('\0') if '=' in v)
                processes.append((int(p.name), args, env.get('DRIVE9_API_KEY')))
        except (OSError, UnicodeError): continue
    rows = []
    for i, group in enumerate(runner.GROUPS):
        mount = runner.MOUNTS[group]
        fs = subprocess.check_output(['findmnt', '-rn', '-M', str(mount), '-o', 'FSTYPE'], text=True).strip()
        assert fs == ('fuse.drive9' if i < 4 else 'nfs4' if group == 'efs' else 'ext4'), group + ' wrong filesystem'
        mount.stat()
        assert os.access(mount, os.W_OK), group + ' not writable'
        if i >= 4: continue
        credential = Path('/home/ubuntu/drive9-sixway-20260911/credentials') / (group + '.json')
        assert credential.stat().st_mode & 0o777 == 0o600, 'Credential mode must be 0600'
        c = json.loads(credential.read_text())
        assert c['tenant_id'] and c['server'] == runner.ENDPOINT
        assert c['tenant_id'] not in {row['space'] for row in rows}, 'Groups require distinct tenants'
        matches = [(pid, args) for pid, args, key in processes if key == c['api_key']]
        assert len(matches) == 1, group + ': require one foreground mount with this credential'
        pid, args = matches[0]
        assert not any(a.startswith('--api-key') for a in args), 'Do not store command-line credentials'
        assert args[-1] == str(mount) and args[-2] == ':/benchmark'
        assert digest(f'/proc/{pid}/exe') == SHA
        expected = {'--profile': 'coding-agent' if group.startswith('coding') else 'none', '--durability': 'fsync' if group.endswith('a') else 'interactive', '--server': runner.ENDPOINT, '--mode': 'fuse', '--gvisor-compat': 'false', '--dir-ttl': '30s', '--attr-ttl': '30s', '--entry-ttl': '30s'}
        assert all(option(args, k) == v for k, v in expected.items()), group + ': mount flags differ'
        assert '--allow-other' in args
        profiling = any(a.startswith(('--perf-', '--debug', '--profile-cpu', '--profile-heap')) for a in args)
        assert allow_profiling or not profiling, group + ': profiling differs from baseline; restore flags with approval or explicitly allow a labelled diagnostic rerun'
        assert not any(a.startswith(('--local-only', '--remote-only', '--trust-process-local-events')) for a in args)
        allowed = set(expected) | {'--foreground', '--allow-other', '--cache-dir', '--local-root'}
        extras = [a.split('=', 1)[0] for a in args if a.startswith('-') and a.split('=', 1)[0] not in allowed]
        assert all(allow_profiling and a.startswith(('--perf-', '--debug', '--profile-cpu', '--profile-heap')) for a in extras), group + ': unrecorded extra flags'
        rows.append({'group': group, 'pid': pid, 'space': c['tenant_id'], 'args': args, 'profiling': profiling})
    m = memory([r['pid'] for r in rows])
    assert m['available_bytes'] >= 4 * 1024**3 and max(m['worker_rss_bytes'].values()) < 2 * 1024**3, 'Insufficient memory headroom'
    (HERE / 'preflight.json').write_text(json.dumps({'workers': rows, 'memory': m, 'allow_profiling': allow_profiling}, indent=2))
    print(json.dumps({'preflight': 'passed', 'profiling_enabled': any(r['profiling'] for r in rows)}), flush=True)
    return [r['pid'] for r in rows]


def stop_tree(pid):
    parent = {}
    for p in Path('/proc').iterdir():
        if not p.name.isdigit(): continue
        try:
            text = (p / 'stat').read_text().rsplit(')', 1)[1].split()
            parent[int(p.name)] = int(text[1])
        except (OSError, ValueError, IndexError): continue
    owned = {pid}
    while True:
        found = {p for p, pp in parent.items() if pp in owned}
        if found <= owned: break
        owned |= found
    for target in sorted(owned - {pid}, reverse=True) + [pid]:
        try: os.kill(target, signal.SIGTERM)
        except ProcessLookupError: pass
    return owned


def guarded(stage, allow_profiling):
    pids = check(allow_profiling)
    marker = HERE / (stage + '-replay-launch.json')
    assert not marker.exists(), 'Stage was launched before; inspect, do not blindly rerun'
    if stage == 'full':
        q = json.loads((HERE / 'atomic-qualification.json').read_text())
        assert q['ready_for_performance'] and len(q['groups']) == 6
    cmd = [sys.executable, 'atomic_probe.py'] if stage == 'atomic' else [sys.executable, 'run_matrix.py', stage]
    with (HERE / (stage + '.log')).open('w') as log, (HERE / (stage + '-memory.jsonl')).open('w', buffering=1) as mem:
        proc = subprocess.Popen(cmd, cwd=HERE, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
        marker.write_text(json.dumps({'pid': proc.pid, 'stage': stage}))
        try:
            while proc.poll() is None:
                r = memory(pids); mem.write(json.dumps(r) + '\n')
                assert r['available_bytes'] >= 2 * 1024**3 and max(r['worker_rss_bytes'].values()) < 2 * 1024**3, 'Memory safety stop'
                time.sleep(1)
            assert proc.returncode == 0, 'Stage failed; retain logs/results and investigate'
        except BaseException:
            stop_tree(proc.pid)
            try: proc.wait(timeout=15)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=5)
            raise
    if stage == 'atomic':
        q = json.loads((HERE / 'atomic-results.json').read_text())
        assert q['complete'] and len(q['rows']) == 6 and all(r['passed'] for r in q['rows'])
        (HERE / 'atomic-qualification.json').write_text(json.dumps({'ready_for_performance': True, 'groups': [r['group'] for r in q['rows']], 'rows': q['rows'], 'note': 'Fresh six-way qualification, not historical retry approval'}, indent=2))
    print('COMPLETED ' + stage)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('command', choices=['prepare', 'check', 'smoke', 'atomic', 'full'])
    p.add_argument('output', nargs='?')
    p.add_argument('--allow-profiling', action='store_true')
    a = p.parse_args()
    if a.command == 'prepare':
        assert a.output
        prepare(HERE / 'original', Path(a.output))
    elif a.command == 'check': check(a.allow_profiling)
    else: guarded(a.command, a.allow_profiling)
