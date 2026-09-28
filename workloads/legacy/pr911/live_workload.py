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
    log=open(RUN/(mode+('-cold' if readonly else '')+'.mount.log'),'wb')
    args=[CLI,'mount','--foreground','--mode=fuse','--profile=coding-agent','--durability=interactive','--flush-debounce=0','--local-root',str(local)]
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
def workload(mnt):
    expected={}
    def grown(name,size,rename=False):
        data=b'H'*4096+b'G'*(size-4096)
        src=mnt/(name+'.tmp' if rename else name)
        with open(src,'wb') as f:f.write(data[:4096]);f.flush()
        if rename:os.rename(src,mnt/name)
        with open(mnt/name,'r+b') as f:f.seek(4096);f.write(data[4096:]);f.flush();os.fsync(f.fileno())
        return name,sha(data),len(data)
    for name,size,rename in [('inline.bin',12288,False),('spill.bin',8<<20,False),('rename.bin',1<<20,True),('large.bin',64<<20,False)]:
        n,h,s=grown(name,size,rename);expected[n]=dict(sha256=h,size=s);print('WRITE',n,s,flush=True)
    # Deterministic parallel files (not unsynchronized LWW to one inode).
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for n,h,s in pool.map(lambda i:grown('concurrent-%d.bin'%i,(256+i*64)<<10),range(8)):
            expected[n]=dict(sha256=h,size=s)
    # Open descriptors survive unlink without publishing the deleted name.
    anonymous=mnt/'anonymous.bin'
    with open(anonymous,'w+b') as f:
        f.write(b'before');f.flush();os.unlink(anonymous);f.write(b'-after');f.flush();os.fsync(f.fileno());f.seek(0)
        if f.read()!=b'before-after':raise AssertionError('anonymous bytes changed')
    if anonymous.exists():raise AssertionError('unlink resurrected path')
    # Normal SQLite/WAL use, plus cold/API-visible database verification later.
    db=mnt/'state.db';conn=sqlite3.connect(db)
    if conn.execute('pragma journal_mode=WAL').fetchone()[0].lower()!='wal':raise AssertionError('not WAL')
    conn.execute('pragma synchronous=FULL');conn.execute('create table records (id integer primary key, value text)')
    for batch in range(8):
        conn.executemany('insert into records(value) values (?)',[(f'row-{batch}-{i}',) for i in range(8)]);conn.commit()
    conn.execute('pragma wal_checkpoint(TRUNCATE)')
    if conn.execute('pragma integrity_check').fetchone()[0]!='ok':raise AssertionError('SQLite integrity')
    conn.close()
    data=db.read_bytes();expected['state.db']=dict(sha256=sha(data),size=len(data),rows=64)
    return expected

for mode in ['normal','gvisor-compat']:
    root='/pr911-d88af4e-'+mode
    for attempt in range(60):
        try:cli('fs','mkdir',root,timeout=15);break
        except Exception:
            if attempt==59:raise
            time.sleep(2)
    mnt=RUN/('mnt-'+mode);p,log=mount(mode,root,RUN/('local-'+mode),mnt)
    try:manifest=workload(mnt)
    finally:stop(p,log,mnt)
    json.dump(manifest,open(RUN/(mode+'.manifest.json'),'w'),indent=2)
    downloaded=RUN/('download-'+mode);downloaded.mkdir(exist_ok=True)
    for name,want in manifest.items():
        local=downloaded/name;cli('fs','cp',':'+root+'/'+name,str(local))
        data=local.read_bytes()
        if len(data)!=want['size'] or sha(data)!=want['sha256']:raise AssertionError('API mismatch '+mode+'/'+name)
        if 'rows' in want:
            db=sqlite3.connect('file:'+str(local)+'?mode=ro',uri=True)
            assert db.execute('pragma integrity_check').fetchone()[0]=='ok'
            assert db.execute('select count(*) from records').fetchone()[0]==want['rows'];db.close()
        print('PASS API',mode,name,want['size'],flush=True)
    # A new local-root rules out a warm cache/shadow-only success.
    p,log=mount(mode,root,RUN/('cold-local-'+mode),mnt,readonly=True)
    try:
        for name,want in manifest.items():
            data=(mnt/name).read_bytes()
            if len(data)!=want['size'] or sha(data)!=want['sha256']:raise AssertionError('cold mismatch '+mode+'/'+name)
        assert not (mnt/'anonymous.bin').exists()
        print('PASS COLD',mode,len(manifest),'files',flush=True)
    finally:stop(p,log,mnt)
print('LIVE WORKLOAD PASS',flush=True)
