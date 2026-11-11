"""Concurrency sweep: fixed 64 client identities, same binaries as client sweep."""
import csv
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

RUN = 'concurrency-20261111-qwUdyA'
REMOTE = '/tmp/' + RUN
BIN = '/tmp/gc-20261111-QtN7Qx'
PRIOR = Path('/home/tsemb_p/Documents/ChatGPT/452/benchmark-results/gc-20261111-QtN7Qx')
LOCAL = PRIOR.parent / RUN
SERVER = 'tsembp@pc713.emulab.net'
CLIENT = 'tsembp@pc708.emulab.net'
CONTROLLER = 'tsembp@pc705.emulab.net'
SSH_OPTIONS = ['-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ControlMaster=auto',
               '-o','ControlPersist=600','-o','ServerAliveInterval=10','-o','ServerAliveCountMax=2',
               '-o','ControlPath=/tmp/rifl-concurrency-qwUdyA/ssh-v2-%C']

def command(args, timeout=90):
    completed = subprocess.run(args,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                               timeout=timeout)
    if completed.returncode:
        raise RuntimeError(f'Command failed ({completed.returncode}): {args}\n{completed.stderr}')
    return completed.stdout

def ssh(host, script, timeout=90):
    return command(['ssh',*SSH_OPTIONS,host,script],timeout)

def download(host, name):
    command(['scp',*SSH_OPTIONS,host+':'+REMOTE+'/'+name,str(LOCAL/name)],300)

def log(message):
    line = time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+' '+message
    print(line,flush=True)
    with (LOCAL/'run.log').open('a') as output:
        output.write(line+'\n')

def setup():
    LOCAL.mkdir(parents=True,exist_ok=False)
    previous = json.loads((PRIOR/'manifest.json').read_text())
    for role,host,files in [('server',SERVER,['server-old','server-new']),
                            ('client',CLIENT,['bench']),('controller',CONTROLLER,[])]:
        ssh(host,'test ! -e '+REMOTE+' && mkdir -m 700 '+REMOTE)
        sums = ssh(host,shlex.join(['sha256sum',*[BIN+'/'+f for f in files]])) if files else ''
        for line in sums.splitlines():
            digest,name = line.split()
            if digest != previous['binary_sha256'][Path(name).name]:
                raise RuntimeError('Remote binary does not match first sweep: '+name)
        environment = ssh(host,'hostname; date -u; uname -a; cat /etc/os-release; lscpu; free -h; '
                          'lsblk -o NAME,MODEL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS; df -hT /tmp; ip -br addr; uptime')
        (LOCAL/(role+'-environment.txt')).write_text(environment)
    if ssh(SERVER,"ss -H -ltn '( sport = :9000 )'").strip():
        raise RuntimeError('Existing listener on port 9000; leaving it untouched.')
    for filename in ['old-source.tar','new-source.tar.gz','optimization.patch']:
        shutil.copy2(PRIOR/filename,LOCAL/filename)
    metadata = {**previous,'run':RUN,'family':'concurrency','previous_run':previous['run'],
                'concurrency':[1,4,16,32,64],'virtual_clients':64,'duration_seconds':15,
                'warmup':2048,'note':'Fixed 64 logical clients isolates in-flight concurrency from client-count growth.',
                'variant_order':'cyclic rotation by worker count and repeat',
                'fresh_database_per_point':True,'reused_binary_directory':BIN}
    (LOCAL/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
    log('Verified identical server/client binaries and captured all node environments.')

def stop(pid,binary):
    ssh(SERVER,'if [ "$(readlink /proc/'+str(pid)+'/exe)" = '+shlex.quote(binary)+' ]; then kill -TERM '+str(pid)+'; fi; '
        'for i in $(seq 1 100); do test ! -e /proc/'+str(pid)+'/exe && exit 0; sleep 0.05; done; exit 1')

def point(variant,workers,repeat):
    mode = 'baseline' if variant=='baseline' else 'protected'
    binary = BIN+('/server-new' if variant=='optimized' else '/server-old')
    tag = f'{variant}-c{workers}-r{repeat}'
    db = REMOTE+'/'+tag+'.db'
    if ssh(SERVER,'test -e '+db+' && echo EXISTS || true').strip():
        raise RuntimeError('Refusing to reuse '+db)
    start = shlex.join([binary,'-mode',mode,'-addr','10.10.1.1:9000','-db',db])
    pid = int(ssh(SERVER,'nohup '+start+' >'+REMOTE+'/'+tag+'.server.log 2>&1 </dev/null & echo $!').strip())
    try:
        ssh(SERVER,'python3 -c '+shlex.quote(
            "import socket,time\nfor i in range(100):\n try:\n  s=socket.create_connection(('10.10.1.1',9000),.1);s.close();break\n except OSError:time.sleep(.05)\nelse:raise SystemExit('readiness timeout')"))
        ssh(SERVER,'nohup sh '+BIN+'/collect-server-metrics.sh '+str(pid)+' '+db+' '+REMOTE+'/'+tag+'.metrics.csv >/dev/null 2>&1 </dev/null &')
        args = [BIN+'/bench','-addr','10.10.1.1:9000','-implementation',mode,'-scenario','normal',
                '-concurrency',str(workers),'-virtual-clients','64','-warmup','2048','-repeats','1',
                '-repeat-start',str(repeat),'-timeout','5s','-payload-bytes','100',
                '-connection-mode','persistent','-seed','452','-duration','15s',
                '-client-prefix',RUN+'-'+tag,'-out',REMOTE+'/'+variant+'-summary.csv',
                '-latency-out',REMOTE+'/'+variant+'-latency.csv','-timeline-out',REMOTE+'/'+variant+'-timeline.csv']
        ssh(CLIENT,shlex.join(args),180)
    finally:
        stop(pid,binary)
    download(CLIENT,variant+'-summary.csv')
    with (LOCAL/(variant+'-summary.csv')).open() as source:
        row = list(csv.DictReader(source))[-1]
    if any(int(row[key]) for key in ['errors','attempt_errors','duplicate_effects']):
        raise RuntimeError('Invalid ordinary measurement: '+str(row))
    if int(row['virtual_clients'])!=64 or int(row['concurrency'])!=workers:
        raise RuntimeError('Wrong workload parameters: '+str(row))
    if variant!='baseline' and int(row['completions_after'])!=64:
        raise RuntimeError('Unexpected protected metadata count: '+str(row))
    log(f'{tag}: {float(row["throughput_ops_s"]):.1f} ops/s, p99={row["p99_ms"]} ms, errors=0')

def collect():
    for host,role in [(SERVER,'server'),(CLIENT,'client')]:
        name = role+'-results.tar.gz'
        patterns = '*.csv *.server.log' if role=='server' else '*.csv'
        ssh(host,'cd '+REMOTE+' && tar -czf '+name+' '+patterns,300)
        download(host,name)
    command(['scp',*SSH_OPTIONS,str(LOCAL/'server-results.tar.gz'),str(LOCAL/'client-results.tar.gz'),
             str(LOCAL/'manifest.json'),CONTROLLER+':'+REMOTE+'/'],300)
    log('Raw results downloaded locally and backed up on controller.')

if __name__=='__main__':
    completed = set()
    if '--resume' in sys.argv:
        if ssh(SERVER,"ss -H -ltn '( sport = :9000 )'").strip():
            raise RuntimeError('Existing listener on resume; inspect before proceeding.')
        for variant in ['baseline','original','optimized']:
            download(CLIENT,variant+'-summary.csv')
            with (LOCAL/(variant+'-summary.csv')).open() as source:
                for row in csv.DictReader(source):
                    if any(int(row[k]) for k in ['errors','attempt_errors','duplicate_effects']):
                        raise RuntimeError('Cannot silently reuse invalid point')
                    completed.add((variant,int(row['concurrency']),int(row['repeat'])))
        metadata = json.loads((LOCAL/'manifest.json').read_text())
        metadata.setdefault('interruptions',[]).append(dict(
            utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),completed_points=len(completed),
            reason='Stalled multiplexed SSH control connection between points, before optimized-c4-r4 started.',
            recovery='Verified server idle and next database absent; used a fresh control socket with keepalives.'))
        (LOCAL/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
        log(f'Resuming after control-connection stall; preserving {len(completed)} completed valid points.')
    else:
        setup()
    shutil.copy2(__file__,LOCAL/'run.py')
    log('Starting 75 concurrency measurements: 15s, five repeats, fixed 64 clients.')
    for n,workers in enumerate([1,4,16,32,64]):
        for repeat in range(1,6):
            variants = ['baseline','original','optimized']
            shift = (n+repeat-1)%3
            for variant in variants[shift:]+variants[:shift]:
                if (variant,workers,repeat) not in completed:
                    point(variant,workers,repeat)
    collect()
    log('Concurrency sweep complete.')
