import concurrent.futures, hashlib, json, os, pathlib, sqlite3, subprocess, sys, time
RUN=pathlib.Path('/home/ubuntu/dev2-pr911-d88af4e')
CLI=str(RUN/'drive9-linux-amd64')
cred=json.load(open(RUN/'provision.json'))
ENV=dict(os.environ,HOME=str(RUN/'home'),TMPDIR=str(RUN/'tmp'),DRIVE9_SERVER=os.environ.get('DRIVE9_SERVER', cred['server']),DRIVE9_API_KEY=cred['api_key'])

def cli(*args,timeout=120):
    r=subprocess.run([CLI,*args],env=ENV,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if r.returncode: raise RuntimeError('CLI '+str(args)+': '+r.stderr.decode()[-1200:])
    return r.stdout

def mount(mode,root,local,mnt,readonly=False):
    mnt.mkdir(exist_ok=True);local.mkdir(exist_ok=True)
    log=open(RUN/(mode+('-cold' if readonly else '')+'.cold-r3.mount.log'),'wb')
    args=[CLI,'mount','--foreground','--mode=fuse','--profile=coding-agent','--durability=interactive','--flush-debounce=0','--local-root',str(local)]
    args += ['--cache-dir',str(CACHE)]
    if mode=='gvisor-compat':args+=['--gvisor-compat']
    if readonly:args+=['--read-only']
    args += [':'+root,str(mnt)]
    p=subprocess.Popen(args,env=ENV,stdout=log,stderr=log)
    for _ in range(120):
        if subprocess.run(['mountpoint','-q',str(mnt)]).returncode==0:return p,log
        if p.poll() is not None:raise RuntimeError('mount failed; inspect '+log.name)
        time.sleep(.25)
    p.terminate();raise RuntimeError('mount timed out')

def stop(p,log,mnt):
    try:cli('umount','--timeout','90s',str(mnt),timeout=110)
    finally:
        if p.poll() is None:
            try:p.wait(timeout=15)
            except subprocess.TimeoutExpired:p.terminate();p.wait(timeout=10)
        log.close()

def sha(data):return hashlib.sha256(data).hexdigest()

CACHE=None
results=[]
for mode in ['normal','gvisor-compat']:
    root='/pr911-9b4e1ea-r2-'+mode
    CACHE=RUN/('empty-read-cache-r3-'+mode)
    local=RUN/('empty-overlay-r3-'+mode)
    assert not CACHE.exists() and not local.exists(), 'fresh cache/overlay must not exist'
    CACHE.mkdir();local.mkdir()
    assert list(CACHE.iterdir())==[] and list(local.iterdir())==[]
    print('EMPTY CACHE VERIFIED',mode,str(CACHE),str(local),flush=True)
    manifest=json.load(open(RUN/(mode+'.r2.manifest.json')))
    mnt=RUN/('mnt-cold-r3-'+mode)
    p,log=mount(mode+'-r3' if mode=='normal' else mode,root,local,mnt,readonly=True)
    try:
        for name,want in manifest.items():
            data=(mnt/name).read_bytes()
            assert len(data)==want['size'] and sha(data)==want['sha256'], name
            print('PASS EMPTY-CACHE READ',mode,name,len(data),flush=True)
        assert not (mnt/'anonymous.bin').exists()
        results.append({'mode':mode,'files':len(manifest),'cache':str(CACHE),'overlay':str(local)})
    finally: stop(p,log,mnt)
json.dump({'commit':'9b4e1eaf83a4a10231e3dfe33dfc3ab67dda55ba','results':results},open(RUN/'cold-r3-summary.json','w'),indent=2)
print('EMPTY CACHE MATRIX COMPLETE',flush=True)
