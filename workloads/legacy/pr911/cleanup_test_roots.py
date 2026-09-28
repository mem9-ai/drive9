import concurrent.futures, hashlib, json, os, pathlib, sqlite3, subprocess, sys, time
RUN=pathlib.Path('/home/ubuntu/dev2-pr911-d88af4e')
CLI=str(RUN/'drive9-f4bacb7-linux-amd64')
cred=json.load(open(RUN/'provision.json'))
ENV=dict(os.environ,HOME=str(RUN/'home'),TMPDIR=str(RUN/'tmp'),DRIVE9_SERVER=os.environ.get('DRIVE9_SERVER', cred['server']),DRIVE9_API_KEY=cred['api_key'])

def cli(*args,timeout=120):
    r=subprocess.run([CLI,*args],env=ENV,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if r.returncode: raise RuntimeError('CLI '+str(args)+': '+r.stderr.decode()[-1200:])
    return r.stdout


roots=['/pr911-d88af4e-normal','/pr911-9b4e1ea-r2-normal','/pr911-9b4e1ea-r2-gvisor-compat','/pr911-anonymous-base-compare','/pr911-anonymous-head-compare','/pr911-anonymous-merge-base-compare','/pr911-layer-9b4e1ea-r1','/pr911-f4bacb7-r4-normal','/pr911-f4bacb7-r4-gvisor-compat','/pr911-layer-f4bacb7-r2']
for mnt in RUN.iterdir():
    if mnt.is_dir() and subprocess.run(['mountpoint','-q',str(mnt)]).returncode==0:
        raise RuntimeError('test mount still active: '+str(mnt))
for root in roots:
    cli('fs','rm','-r',root,timeout=120)
    print('REMOVED TEST ROOT',root,flush=True)
print('ISOLATED TEST ROOT CLEANUP COMPLETE',flush=True)
