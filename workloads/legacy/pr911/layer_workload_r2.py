import concurrent.futures, hashlib, json, os, pathlib, sqlite3, subprocess, sys, time
RUN=pathlib.Path('/home/ubuntu/dev2-pr911-d88af4e')
CLI=str(RUN/'drive9-f4bacb7-linux-amd64')
cred=json.load(open(RUN/'provision.json'))
ENV=dict(os.environ,HOME=str(RUN/'home'),TMPDIR=str(RUN/'tmp'),DRIVE9_SERVER=os.environ.get('DRIVE9_SERVER', cred['server']),DRIVE9_API_KEY=cred['api_key'])

def cli(*args,timeout=120):
    r=subprocess.run([CLI,*args],env=ENV,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if r.returncode: raise RuntimeError('CLI '+str(args)+': '+r.stderr.decode()[-1200:])
    return r.stdout

def mount(mode,root,local,mnt,readonly=False):
    mnt.mkdir(exist_ok=True)
    cache=RUN/('empty-cache-'+mode+('-cold' if readonly else '-warm'))
    assert not local.exists() and not cache.exists(), 'cache/overlay must be fresh'
    local.mkdir();cache.mkdir()
    print('EMPTY CACHE VERIFIED',str(cache),str(local),flush=True)
    log=open(RUN/(mode+('-cold' if readonly else '')+'.r2.mount.log'),'wb')
    args=[CLI,'mount','--foreground','--mode=fuse','--profile=coding-agent','--durability=interactive','--flush-debounce=0','--local-root',str(local)]
    args += ['--layer', LAYER, '--cache-dir',str(cache)]
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

LAYER=''
root='/pr911-layer-f4bacb7-r2'
cli('fs','mkdir',root)
created=json.loads(cli('fs','layer','create','--name','pr911-layer-f4bacb7-r2','--json',':'+root))
LAYER=created['layer_id']
manifest={}
mnt=RUN/'mnt-layer-r2'
p,log=mount('layer-r2',root,RUN/'local-layer-r2',mnt)
try:
    for name,size in [('inline.bin',12288),('stream.bin',8<<20),('large.bin',64<<20)]:
        data=b'H'*4096+b'L'*(size-4096)
        with open(mnt/name,'wb') as f:
            f.write(data[:4096]);f.flush();os.fsync(f.fileno())
            f.write(data[4096:]);f.flush();os.fsync(f.fileno())
        assert (mnt/name).read_bytes()==data
        manifest[name]={'size':size,'sha256':sha(data)}
        print('PASS LAYER GROW',name,size,flush=True)
finally:stop(p,log,mnt)
print('LAYER DIFF',cli('fs','layer','diff','--json',LAYER).decode(),flush=True)
p,log=mount('layer-r2',root,RUN/'cold-local-layer-r2',mnt,readonly=True)
try:
    for name,want in manifest.items():
        data=(mnt/name).read_bytes()
        assert len(data)==want['size'] and sha(data)==want['sha256']
    print('PASS LAYER COLD',len(manifest),flush=True)
finally:stop(p,log,mnt)
print('COMMIT',cli('fs','layer','commit',LAYER,timeout=180).decode(),flush=True)
for name,want in manifest.items():
    dest=RUN/('layer-api-'+name)
    cli('fs','cp',':'+root+'/'+name,str(dest))
    data=dest.read_bytes()
    assert len(data)==want['size'] and sha(data)==want['sha256']
    print('PASS LAYER COMMITTED API',name,flush=True)
json.dump({'commit':'f4bacb7390e820dc8d1f474011c9e09614fe2494','layer_id':LAYER,'root':root,'manifest':manifest},open(RUN/'layer-r2-summary.json','w'),indent=2)
print('LAYER MATRIX COMPLETE',flush=True)
