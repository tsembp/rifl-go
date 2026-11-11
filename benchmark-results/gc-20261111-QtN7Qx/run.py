"""Run isolated CloudLab comparisons; all generated files stay outside the repo."""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import time

TASK = Path('/tmp/rifl-cloudlab-QtN7Qx')
RUN = 'gc-20261111-QtN7Qx'
REMOTE = '/tmp/' + RUN
LOCAL = Path('/home/tsemb_p/Documents/ChatGPT/452/benchmark-results') / RUN
SERVER = 'tsembp@pc713.emulab.net'
CLIENT = 'tsembp@pc708.emulab.net'
CONTROLLER = 'tsembp@pc705.emulab.net'
SSH_OPTIONS = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
               '-o', 'ControlMaster=auto', '-o', 'ControlPersist=600',
               '-o', 'ControlPath=' + str(TASK / 'ssh-%C')]

def command(args, timeout=90):
    return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, check=True).stdout

def ssh(host, script, timeout=90):
    return command(['ssh', *SSH_OPTIONS, host, script], timeout)

def upload(host, *files):
    command(['scp', *SSH_OPTIONS, *map(str, files), host + ':' + REMOTE + '/'])

def download(host, name, destination):
    command(['scp', *SSH_OPTIONS, host + ':' + REMOTE + '/' + name, str(destination)], 300)

def log(message):
    line = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + ' ' + message
    print(line, flush=True)
    with (LOCAL / 'run.log').open('a') as output:
        output.write(line + '\n')

def setup():
    LOCAL.mkdir(parents=True, exist_ok=False)
    for host in (SERVER, CLIENT, CONTROLLER):
        ssh(host, 'test ! -e ' + REMOTE + ' && mkdir -m 700 ' + REMOTE)
    if ssh(SERVER, "ss -H -ltn '( sport = :9000 )'").strip():
        raise RuntimeError('Port 9000 already in use; leave existing process untouched.')
    upload(SERVER, TASK/'bin/server-old', TASK/'bin/server-new', TASK/'new/scripts/cloudlab/collect-server-metrics.sh')
    upload(CLIENT, TASK/'bin/bench')
    upload(CONTROLLER, TASK/'bin/benchplot')
    for filename in ['old-source.tar', 'new-source.tar.gz', 'optimization.patch']:
        shutil.copy2(TASK/filename, LOCAL/filename)
    binaries = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in (TASK/'bin').iterdir()}
    metadata = dict(run=RUN, base_commit='6a50b38ed54ec2c8602893a0c42cb47b1879e076',
                    optimized='uncommitted source snapshot in new-source.tar.gz',
                    compiler=command(['go','version']).strip(), buildvcs=False, cgo=False,
                    binary_sha256=binaries, server=SERVER, client=CLIENT, controller=CONTROLLER,
                    private_server='10.10.1.1:9000', duration_seconds=15, repeats=5,
                    concurrency=4, virtual_clients=[4,16,64,256,1024], payload_bytes=100,
                    warmup=2048, connection_mode='persistent', storage_sync='bbolt defaults',
                    note='Warmup 2048 initializes every virtual client, unlike old warmup 1000 at 1024 clients.')
    (LOCAL/'manifest.json').write_text(json.dumps(metadata, indent=2)+'\n')
    for role, host in [('server',SERVER),('client',CLIENT),('controller',CONTROLLER)]:
        environment = ssh(host, 'hostname; date -u; cat /etc/os-release; uname -a; lscpu; free -h; '
                           'lsblk -o NAME,MODEL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS; df -hT /tmp; '
                           'ip -br addr; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true')
        (LOCAL/(role+'-environment.txt')).write_text(environment)
    log('Deployed original/protected-v1, optimized/protected-v2, and common benchmark binary.')

def stop_server(pid, binary):
    ssh(SERVER, 'if [ "$(readlink /proc/'+str(pid)+'/exe)" = '+shlex.quote(binary)+' ]; then kill -TERM '+str(pid)+'; fi; '
        'for attempt in $(seq 1 100); do test ! -e /proc/'+str(pid)+'/exe && exit 0; sleep 0.05; done; exit 1')

def point(variant, clients, repeat, smoke=False):
    implementation = 'baseline' if variant == 'baseline' else 'protected'
    executable = 'server-new' if variant == 'optimized' else 'server-old'
    binary = REMOTE+'/'+executable
    tag = ('smoke-' if smoke else '')+f'{variant}-v{clients}-r{repeat}'
    prefix = ('smoke-' if smoke else '')+variant
    db = REMOTE+'/'+tag+'.db'
    if ssh(SERVER, 'test -e '+db+' && echo EXISTS || true').strip():
        raise RuntimeError('Refusing to reuse database '+db)
    start = shlex.join([binary,'-mode',implementation,'-addr','10.10.1.1:9000','-db',db])
    pid = int(ssh(SERVER, 'nohup '+start+' >'+REMOTE+'/'+tag+'.server.log 2>&1 </dev/null & echo $!').strip())
    try:
        # Wait for the actual listener, not a fixed two-second delay.
        ssh(SERVER, "python3 -c " + shlex.quote(
            "import socket,time\nfor i in range(100):\n try:\n  s=socket.create_connection(('10.10.1.1',9000),.1);s.close();break\n except OSError:time.sleep(.05)\nelse:raise SystemExit('server readiness timeout')"))
        ssh(SERVER, 'nohup sh '+REMOTE+'/collect-server-metrics.sh '+str(pid)+' '+db+' '+REMOTE+'/'+tag+'.metrics.csv >/dev/null 2>&1 </dev/null &')
        args = [REMOTE+'/bench','-addr','10.10.1.1:9000','-implementation',implementation,
                '-scenario','normal','-concurrency','4','-virtual-clients',str(clients),
                '-warmup','2048' if not smoke else '16','-repeats','1','-repeat-start',str(repeat),
                '-timeout','5s','-payload-bytes','100','-connection-mode','persistent','-seed','452',
                '-client-prefix',RUN+'-'+tag,'-out',REMOTE+'/'+prefix+'-summary.csv',
                '-latency-out',REMOTE+'/'+prefix+'-latency.csv','-timeline-out',REMOTE+'/'+prefix+'-timeline.csv']
        args += ['-requests','200'] if smoke else ['-duration','15s']
        ssh(CLIENT, shlex.join(args), timeout=180)
    finally:
        stop_server(pid,binary)
    # Keep summary progress on the laptop after every point.
    download(CLIENT,prefix+'-summary.csv',LOCAL/(prefix+'-summary.csv'))
    with (LOCAL/(prefix+'-summary.csv')).open() as source:
        row = list(csv.DictReader(source))[-1]
    if int(row['errors']) or int(row['attempt_errors']) or int(row['duplicate_effects']):
        raise RuntimeError('Invalid ordinary performance point: '+str(row))
    if variant != 'baseline' and int(row['completions_after']) != min(clients,200 if smoke else clients):
        raise RuntimeError('Unexpected protected completion count: '+str(row))
    log(f'{tag}: {float(row["throughput_ops_s"]):.1f} ops/s, p99={row["p99_ms"]} ms, errors=0')

def collect():
    for host, role in [(SERVER,'server'),(CLIENT,'client')]:
        archive = role+'-results.tar.gz'
        patterns = '*.csv *.server.log' if role=='server' else '*.csv'
        ssh(host, 'cd '+REMOTE+' && tar -czf '+archive+' '+patterns, timeout=300)
        download(host,archive,LOCAL/archive)
    # Controller gets a second copy of the raw-data archives and provenance.
    upload(CONTROLLER, LOCAL/'server-results.tar.gz',LOCAL/'client-results.tar.gz',LOCAL/'manifest.json')
    log('Downloaded raw results and stored a second copy on the controller.')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase',choices=['setup-smoke','matrix','collect'])
    phase = parser.parse_args().phase
    if phase=='setup-smoke':
        setup()
        for variant in ['baseline','original','optimized']:
            point(variant,4,1,True)
        log('Smoke checks passed for all three variants.')
    elif phase=='matrix':
        log('Starting client-count matrix: 75 points, 15s each, 5 repetitions; warmup=2048.')
        for n,clients in enumerate([4,16,64,256,1024]):
            for repeat in range(1,6):
                variants = ['baseline','original','optimized']
                shift = (n+repeat-1)%3
                for variant in variants[shift:]+variants[:shift]:
                    point(variant,clients,repeat)
        collect()
        log('Client-count matrix complete.')
    else:
        collect()

if __name__=='__main__':
    main()
