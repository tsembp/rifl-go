#!/usr/bin/env python3
"""Compare configurations A, B and C from SHM_DESIGN_PROPOSAL.md on ONE node.

  A  TCP net/rpc (loopback) + bbolt   cmd/server
  B  shm slots + bbolt                cmd/shmserver -store bbolt
  C  shm slots + shm RIFL state       cmd/shmserver -store shm

All three run server and benchmark on the same machine, because C cannot span
hosts in this model; A's numbers are therefore loopback numbers. bbolt files go
to --data-dir (a real disk); regions go to /dev/shm.

Phases (resumable; refuses to reuse a database or region):
  build setup smoke throughput batch failover collect

Example:
  GO=go1.27.0 scripts/cloudlab/shm_compare.py --node me@pc1.emulab.net \\
      --run-id shm-20261114 build setup smoke throughput batch failover collect
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import re
import time
from datetime import datetime, timezone

REPO = Path(__file__).resolve().parents[2]
GO = os.environ.get('GO', 'go')
CONFIGS = ['A', 'B', 'C']
LOG_TIME = re.compile(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6}) ')


def parse_log_time(line):
    match = LOG_TIME.match(line)
    if not match:
        raise ValueError('log line without a microsecond UTC timestamp: ' + line)
    return datetime.strptime(match.group(1), '%Y/%m/%d %H:%M:%S.%f').replace(tzinfo=timezone.utc)


def takeover_times(config, kill_text, standby_log):
    """Milliseconds from the recorded kill to the standby holding the lock and
    to it serving. A: lock line, then the listening line. B/C: one line logged
    when ready, which also carries the acquisition time."""
    whole, _, fraction = kill_text.strip().rstrip('Z').partition('.')
    kill = datetime.strptime(whole, '%Y-%m-%dT%H:%M:%S').replace(
        tzinfo=timezone.utc, microsecond=int((fraction + '000000')[:6]))
    lines = standby_log.splitlines()
    if config == 'A':
        locked = parse_log_time(next(l for l in lines if 'acquired standby lock' in l))
        ready = parse_log_time(next(l for l in lines if 'listening on' in l))
    else:
        line = next(l for l in lines if 'owner epoch' in l and 'ready after' in l)
        ready = parse_log_time(line)
        stamp = re.search(r'acquired at (\S+?),', line).group(1)
        whole, _, fraction = stamp.rstrip('Z').partition('.')
        locked = datetime.strptime(whole, '%Y-%m-%dT%H:%M:%S').replace(
            tzinfo=timezone.utc, microsecond=int((fraction + '000000')[:6]))
    ms = lambda t: round((t - kill).total_seconds() * 1000, 3)
    return ms(locked), ms(ready)


class Runner:
    def __init__(self, a):
        self.a = a
        self.local = Path(a.out or REPO / 'benchmark-results' / a.run_id)
        self.build_dir = self.local / 'build'
        self.remote = '/tmp/' + a.run_id
        self.data = a.data_dir.rstrip('/') + '/' + a.run_id
        self.control = Path(os.environ.get('XDG_RUNTIME_DIR', '/tmp')) / ('rifl-ssh-' + a.run_id)
        self.ssh_options = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                            '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4',
                            '-o', 'ControlMaster=auto', '-o', 'ControlPersist=600',
                            '-o', 'ControlPath=' + str(self.control) + '-%C']
        self.done_file = self.local / 'completed-points.txt'

    def run(self, args, timeout=120, **kw):
        return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout, check=True, **kw).stdout

    def ssh(self, script, timeout=120):
        return self.run(['ssh', *self.ssh_options, self.a.node, script], timeout)

    def download(self, name, dest=None):
        self.run(['scp', *self.ssh_options, f'{self.a.node}:{self.remote}/{name}',
                  str(dest or self.local / name)], 600)

    def log(self, message):
        line = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()) + ' ' + message
        print(line, flush=True)
        with (self.local / 'run.log').open('a') as out:
            out.write(line + '\n')

    def completed(self):
        return set(self.done_file.read_text().split()) if self.done_file.exists() else set()

    def mark(self, tag):
        with self.done_file.open('a') as out:
            out.write(tag + '\n')

    # ---- phases ---------------------------------------------------------
    def build(self):
        if self.build_dir.exists():
            raise RuntimeError(f'{self.build_dir} exists; refusing to rebuild over measured binaries')
        self.build_dir.mkdir(parents=True)
        env = {**os.environ, 'CGO_ENABLED': '0', 'GOOS': 'linux', 'GOARCH': 'amd64'}
        for name, pkg in [('rifl-server', './cmd/server'), ('rifl-shmserver', './cmd/shmserver'),
                          ('rifl-bench', './cmd/bench')]:
            self.run([GO, 'build', '-trimpath', '-buildvcs=false', '-o', str(self.build_dir / name), pkg],
                     600, cwd=REPO, env=env)
        (self.build_dir / 'working-tree.patch').write_text(self.run(['git', '-C', str(REPO), 'diff', 'HEAD']))
        files = [f for f in self.run(['git', '-C', str(REPO), 'ls-files', '-co', '--exclude-standard',
                                      '--', '*.go', 'go.mod', 'go.sum', 'scripts']).split()
                 if not f.startswith(('graphify-out/', 'benchmark-results/'))]
        self.run(['tar', '-czf', str(self.build_dir / 'source.tar.gz'), '-C', str(REPO), *files])
        info = dict(head=self.run(['git', '-C', str(REPO), 'rev-parse', 'HEAD']).strip(),
                    note='HEAD plus working-tree.patch; exact source in source.tar.gz',
                    compiler=self.run([GO, 'version']).strip(), cgo=False, buildvcs=False,
                    sha256={f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                            for f in sorted(self.build_dir.iterdir()) if f.is_file()})
        (self.build_dir / 'build.json').write_text(json.dumps(info, indent=2) + '\n')
        print(json.dumps(info, indent=2))

    def setup(self):
        self.ssh(f'mkdir -p -m 700 {self.remote} {self.data}')
        if self.ssh("ss -H -ltn '( sport = :9000 )'").strip():
            raise RuntimeError('port 9000 in use on the node; inspect it first')
        bins = [self.build_dir / n for n in ('rifl-server', 'rifl-shmserver', 'rifl-bench')]
        self.run(['scp', *self.ssh_options, *map(str, bins), str(REPO / 'scripts/shm/failover.sh'),
                  f'{self.a.node}:{self.remote}/'], 300)
        self.ssh(f'chmod +x {self.remote}/rifl-* {self.remote}/failover.sh')
        env = self.ssh('hostname; date -u; cat /etc/os-release; uname -a; lscpu; free -h; '
                       'lsblk -o NAME,MODEL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS; '
                       f'df -hT {self.data} /dev/shm; '
                       'cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true')
        (self.local / 'node-environment.txt').write_text(env)
        manifest = dict(run=self.a.run_id, node=self.a.node, data_dir=self.data, region_dir='/dev/shm',
                        build=json.loads((self.build_dir / 'build.json').read_text()),
                        configs={'A': 'TCP loopback + bbolt', 'B': 'shm slots + bbolt', 'C': 'shm slots + shm state'},
                        throughput=dict(clients=[4, 1024], workers=4, duration_seconds=15, warmup=2048,
                                        repeats=self.a.repeats, payload_bytes=100),
                        batch=dict(records_per_client=[100, 1000, 10000], workers=[1, 4], repeats=self.a.repeats),
                        failover=dict(duration='10s', kill_after_seconds=5, workers=4, clients=4,
                                      repeats=self.a.repeats, signal='SIGKILL'),
                        storage_sync='bbolt defaults', order='rotates by point and repeat')
        (self.local / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        self.log('setup complete')

    # ---- one point -------------------------------------------------------
    def point(self, config, tag, bench_args, summary, expect, clients, records):
        if tag in self.completed():
            return
        db, region = f'{self.data}/{tag}.db', f'/dev/shm/rifl-{self.a.run_id}-{tag}'
        if self.ssh(f'test -e {db} -o -e {region} && echo EXISTS || true').strip():
            raise RuntimeError(f'refusing to reuse state for {tag}; inspect it')
        slots = int(bench_args[bench_args.index('-concurrency') + 1]) + 1
        if config == 'A':
            start = [f'{self.remote}/rifl-server', '-addr', '127.0.0.1:9000', '-db', db]
            transport = ['-transport', 'tcp', '-addr', '127.0.0.1:9000', '-connection-mode', 'persistent']
            ready = "python3 -c \"import socket,time\nfor i in range(200):\n try:\n  socket.create_connection(('127.0.0.1',9000),.1).close();break\n except OSError:time.sleep(.05)\nelse:raise SystemExit('not ready')\""
        else:
            start = [f'{self.remote}/rifl-shmserver', '-region', region, '-create', '-slots', str(slots),
                     '-clients', str(clients), '-records', str(records),
                     '-store', 'bbolt' if config == 'B' else 'shm', '-db', db]
            transport = ['-transport', 'shm', '-region', region]
            ready = (f'for i in $(seq 1 200); do grep -q "owner epoch" {self.remote}/{tag}.server.log && exit 0; '
                     'sleep 0.05; done; exit 1')
        pid = int(self.ssh(f'nohup {shlex.join(start)} >{self.remote}/{tag}.server.log 2>&1 </dev/null & echo $!').strip())
        try:
            self.ssh(ready)
            args = [f'{self.remote}/rifl-bench', *transport, '-implementation', 'protected',
                    '-repeats', '1', '-timeout', '5s', '-payload-bytes', '100', '-seed', '452',
                    '-client-prefix', f'{self.a.run_id}-{tag}',
                    '-out', f'{self.remote}/{summary}-summary.csv',
                    '-latency-out', f'{self.remote}/{summary}-latency.csv',
                    '-timeline-out', f'{self.remote}/{summary}-timeline.csv', *bench_args]
            self.ssh(shlex.join(args), 900)
        finally:
            self.ssh(f'kill -TERM {pid} 2>/dev/null; for i in $(seq 1 200); do test ! -e /proc/{pid} && break; sleep 0.05; done; '
                     f'rm -f {shlex.quote(region)}')
        self.download(f'{summary}-summary.csv')
        with (self.local / f'{summary}-summary.csv').open() as source:
            row = list(csv.DictReader(source))[-1]
        problems = [k for k in ('errors', 'attempt_errors', 'duplicate_effects') if int(row[k])] + expect(row)
        if problems:
            self.log(f'{tag}: INVALID {problems}: {row}')
            raise RuntimeError(f'{tag} failed validation; stopping so it can be documented')
        self.mark(tag)
        self.log(f'{tag}: {float(row["throughput_ops_s"]):.1f} ops/s p50={row["p50_ms"]}ms p99={row["p99_ms"]}ms '
                 f'ack_p50={row["batch_ack_p50_ms"]}ms completions={row["completions_after"]}')

    def ordered(self, n):
        shift = n % len(CONFIGS)
        return CONFIGS[shift:] + CONFIGS[:shift]

    def smoke(self):
        for config in CONFIGS:
            self.point(config, f'smoke-{config}', ['-scenario', 'normal', '-requests', '400', '-concurrency', '4',
                       '-virtual-clients', '4', '-warmup', '16'], 'smoke',
                       lambda r: [] if int(r['completions_after']) == 4 else ['completions'], 8, 64)
        self.log('smoke passed for A, B and C')

    def throughput(self):
        n = 0
        for clients in [4, 1024]:
            for repeat in range(1, self.a.repeats + 1):
                n += 1
                for config in self.ordered(n):
                    self.point(config, f'thr-{config}-v{clients}-r{repeat}',
                               ['-scenario', 'normal', '-duration', '15s', '-concurrency', '4',
                                '-virtual-clients', str(clients), '-warmup', '2048', '-repeat-start', str(repeat)],
                               f'throughput-{config}',
                               lambda r, c=clients: [] if int(r['completions_after']) == c else ['completions_after'],
                               2 * clients, 64)
        self.log('throughput complete')

    def batch(self):
        n = 0
        for records in [100, 1000, 10000]:
            for workers in [1, 4]:
                for repeat in range(1, self.a.repeats + 1):
                    n += 1
                    for config in self.ordered(n):
                        self.point(config, f'batch-{config}-b{records}-w{workers}-r{repeat}',
                                   ['-scenario', 'batch-ack', '-requests', str(records * workers),
                                    '-concurrency', str(workers), '-virtual-clients', str(workers),
                                    '-warmup', '0', '-repeat-start', str(repeat), '-verify-batch-ack'],
                                   f'batch-{config}',
                                   lambda r, w=workers: [] if int(r['completions_after']) == w else ['completions_after'],
                                   2 * workers, records + 2)
        self.log('batch complete')

    def failover(self):
        for repeat in range(1, self.a.repeats + 1):
            for config in self.ordered(repeat):
                tag = f'failover-{config}-r{repeat}'
                if tag in self.completed():
                    continue
                self.ssh(f'cd {self.remote} && DURATION=10s KILL_AFTER=5 DATA_DIR={self.data} '
                         f'LATENCY={int(self.a.failover_latency)} '
                         f'./failover.sh {config} {self.remote} {self.remote}/{tag} >/dev/null', 300)
                self.download(f'{tag}/summary.csv', self.local / f'{tag}-summary.csv')
                locked, ready = takeover_times(config, self.ssh(f'cat {self.remote}/{tag}/kill-time.txt'),
                                               self.ssh(f'cat {self.remote}/{tag}/standby.log'))
                with (self.local / f'{tag}-summary.csv').open() as source:
                    row = list(csv.DictReader(source))[-1]
                if int(row['errors']) or int(row['duplicate_effects']):
                    self.log(f'{tag}: INVALID {row}')
                    raise RuntimeError(f'{tag} failed validation')
                takeover = self.local / 'failover-takeover.csv'
                new = not takeover.exists()
                with takeover.open('a', newline='') as sink:
                    writer = csv.writer(sink)
                    if new:
                        writer.writerow(['config', 'repeat', 'kill_to_lock_ms', 'kill_to_ready_ms',
                                         'max_request_ms', 'p50_ms', 'retried_attempts'])
                    writer.writerow([config, repeat, locked, ready, row['max_ms'], row['p50_ms'],
                                     row['attempt_errors']])
                self.mark(tag)
                self.log(f'{tag}: max={row["max_ms"]}ms p50={row["p50_ms"]}ms lock={locked}ms '
                         f'ready={ready}ms retried_attempts={row["attempt_errors"]} errors=0 duplicates=0')
        self.log('failover complete')

    def collect(self):
        self.ssh(f'cd {self.remote} && tar -czf results.tar.gz $(ls -d *.csv *.log failover-* 2>/dev/null)', 600)
        self.download('results.tar.gz')
        shutil.copy2(__file__, self.local / 'shm_compare.py')
        self.log('raw results downloaded')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--node', required=True, help='user@host running servers and benchmark')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--data-dir', default='/var/tmp', help='disk directory for bbolt files')
    parser.add_argument('--out', help='local results folder (default benchmark-results/<run-id>)')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--no-failover-latency', dest='failover_latency', action='store_false',
                        help='skip per-request latency files in failover runs (summary keeps p50..max)')
    parser.add_argument('phases', nargs='+',
                        choices=['build', 'setup', 'smoke', 'throughput', 'batch', 'failover', 'collect'])
    args = parser.parse_args()
    runner = Runner(args)
    runner.local.mkdir(parents=True, exist_ok=True)
    for phase in args.phases:
        getattr(runner, phase)()


if __name__ == '__main__':
    main()
