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
    log=open(RUN/(mode+('-cold' if readonly else '')+'.r2.mount.log'),'wb')
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

for label,binary in [('base','drive9-base-linux-amd64'),('head','drive9-linux-amd64')]:
    CLI=str(RUN/binary)
    root='/pr911-anonymous-'+label+'-compare'
    cli('fs','mkdir',root)
    mnt=RUN/('anonymous-'+label)
    p,log=mount('anonymous-'+label,root,RUN/('anonymous-local-'+label),mnt)
    try:
        path=mnt/'fd.bin'
        with open(path,'w+b') as f:
            f.write(b'before');f.flush();os.unlink(path);f.write(b'-after');f.flush();os.fsync(f.fileno());f.seek(0)
            got=f.read()
        print(label,'expected_hex',b'before-after'.hex(),'actual_hex',got.hex(),'path_exists',path.exists(),flush=True)
    finally: stop(p,log,mnt)
