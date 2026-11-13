#!/usr/bin/env python3
"""Paired original-vs-optimized runs for HANDOFF_GC_FOLLOWUP.md.

Phases (run in order; each is resumable and refuses to overwrite data):
  build    build original (6a50b38) and optimized (working tree) binaries locally
  setup    upload binaries, capture environments, write manifest.json
  smoke    one tiny point per variant and scenario
  task1    batch-ack GC comparison plus ack-gc/blocked-gap sanity comparison
  task2    four-client/four-worker regression reproduction (uninstrumented)
  profile  separate diagnostic runs: CPU/alloc profiles and per-process I/O
  collect  download raw CSVs, logs and profiles, keep a copy on the controller

Example:
  GO=go1.27.0 scripts/cloudlab/gc_followup.py --server me@pc1.emulab.net \\
      --client me@pc2.emulab.net --controller me@pc3.emulab.net \\
      --run-id gcfollowup-20261112 build setup smoke task1 task2 profile collect
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
import time

REPO = Path(__file__).resolve().parents[2]
GO = os.environ.get('GO', 'go')  # e.g. GO=go1.27.0 to match earlier runs
ORIGINAL_COMMIT = '6a50b38ed54ec2c8602893a0c42cb47b1879e076'
VARIANTS = ['original', 'optimized']


class Runner:
    def __init__(self, args):
        self.a = args
        self.local = Path(args.out or REPO / 'benchmark-results' / args.run_id)
        self.build_dir = self.local / 'build'
        self.remote = '/tmp/' + args.run_id
        self.addr = args.server_ip + ':9000'
        self.control = Path(os.environ.get('XDG_RUNTIME_DIR', '/tmp')) / ('rifl-ssh-' + args.run_id)
        self.ssh_options = ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                            '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4',
                            '-o', 'ControlMaster=auto', '-o', 'ControlPersist=600',
                            '-o', 'ControlPath=' + str(self.control) + '-%C']
        self.done_file = self.local / 'completed-points.txt'

    # ---- plumbing -------------------------------------------------------
    def run(self, args, timeout=120, **kw):
        return subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=timeout, check=True, **kw).stdout

    def ssh(self, host, script, timeout=120):
        return self.run(['ssh', *self.ssh_options, host, script], timeout)

    def upload(self, host, *files):
        self.run(['scp', *self.ssh_options, *map(str, files), host + ':' + self.remote + '/'], 300)

    def download(self, host, name, dest=None):
        self.run(['scp', *self.ssh_options, host + ':' + self.remote + '/' + name,
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
        self.local.mkdir(parents=True, exist_ok=True)
        if self.build_dir.exists():
            raise RuntimeError(f'{self.build_dir} exists; refusing to rebuild over measured binaries')
        self.build_dir.mkdir()
        env = {**os.environ, 'CGO_ENABLED': '0', 'GOOS': 'linux', 'GOARCH': 'amd64'}
        flags = [GO, 'build', '-trimpath', '-buildvcs=false', '-o']
        old_tree = self.build_dir / 'original-src'
        self.run(['git', '-C', str(REPO), 'worktree', 'add', '--detach', str(old_tree), ORIGINAL_COMMIT])
        try:
            # cmd/server is identical in both commits; reuse the current one so both
            # variants get the same opt-in -cpuprofile/-memprofile flags.
            shutil.copy2(REPO / 'cmd/server/main.go', old_tree / 'cmd/server/main.go')
            self.run([*flags, str(self.build_dir / 'server-old'), './cmd/server'], 600, cwd=old_tree, env=env)
        finally:
            self.run(['git', '-C', str(REPO), 'worktree', 'remove', '--force', str(old_tree)])
        self.run([*flags, str(self.build_dir / 'server-new'), './cmd/server'], 600, cwd=REPO, env=env)
        self.run([*flags, str(self.build_dir / 'bench'), './cmd/bench'], 600, cwd=REPO, env=env)
        (self.build_dir / 'optimized-working-tree.patch').write_text(
            self.run(['git', '-C', str(REPO), 'diff', 'HEAD']))
        # The patch omits untracked files, so also archive the exact measured source.
        files = [f for f in self.run(['git', '-C', str(REPO), 'ls-files', '-co', '--exclude-standard',
                                      '--', '*.go', 'go.mod', 'go.sum', 'scripts']).split()
                 if not f.startswith(('graphify-out/', 'benchmark-results/'))]
        self.run(['tar', '-czf', str(self.build_dir / 'optimized-source.tar.gz'), '-C', str(REPO), *files])
        hashes = {f.name: hashlib.sha256(f.read_bytes()).hexdigest()
                  for f in sorted(self.build_dir.iterdir()) if f.is_file()}
        info = dict(original_commit=ORIGINAL_COMMIT,
                    optimized_commit=self.run(['git', '-C', str(REPO), 'rev-parse', 'HEAD']).strip(),
                    optimized_note='HEAD plus optimized-working-tree.patch; exact source in optimized-source.tar.gz (bench verification, server profiling flags)',
                    original_note='6a50b38 with the current cmd/server/main.go (identical except opt-in profiling flags)',
                    compiler=self.run([GO, 'version']).strip(), cgo=False, buildvcs=False,
                    sha256=hashes)
        (self.build_dir / 'build.json').write_text(json.dumps(info, indent=2) + '\n')
        print(json.dumps(info, indent=2))
        if 'go1.27.0' not in info['compiler']:
            print('WARNING: previous paired runs used Go 1.27.0; record this difference in the report.')

    def setup(self):
        if not (self.build_dir / 'build.json').exists():
            raise RuntimeError('run the build phase first')
        for host in (self.a.server, self.a.client, self.a.controller):
            self.ssh(host, 'mkdir -p -m 700 ' + self.remote)
        if self.ssh(self.a.server, "ss -H -ltn '( sport = :9000 )'").strip():
            raise RuntimeError('port 9000 already in use on the server; inspect it first')
        self.upload(self.a.server, self.build_dir / 'server-old', self.build_dir / 'server-new',
                    REPO / 'scripts/cloudlab/collect-server-metrics.sh')
        self.upload(self.a.client, self.build_dir / 'bench')
        self.ssh(self.a.server, f'chmod +x {self.remote}/server-old {self.remote}/server-new')
        self.ssh(self.a.client, f'chmod +x {self.remote}/bench')
        for role, host in [('server', self.a.server), ('client', self.a.client), ('controller', self.a.controller)]:
            env = self.ssh(host, 'hostname; date -u; cat /etc/os-release; uname -a; lscpu; free -h; '
                                 'lsblk -o NAME,MODEL,SIZE,ROTA,TYPE,FSTYPE,MOUNTPOINTS; df -hT /tmp; '
                                 'ip -br addr; cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || true')
            (self.local / f'{role}-environment.txt').write_text(env)
        build = json.loads((self.build_dir / 'build.json').read_text())
        manifest = dict(run=self.a.run_id, family='gc-followup', handoff='HANDOFF_GC_FOLLOWUP.md',
                        server=self.a.server, client=self.a.client, controller=self.a.controller,
                        private_server=self.addr, storage_sync='bbolt defaults',
                        connection_mode='persistent', payload_bytes=100, build=build,
                        task1=dict(scenario='batch-ack', records_per_client=[100, 1000, 10000],
                                   workers=[1, 4], repeats=self.a.repeats, warmup=0,
                                   requests='records_per_client * workers (bench -requests is a total)',
                                   virtual_clients='workers', verify='bench -verify-batch-ack'),
                        task1_sanity=dict(scenarios=['ack-gc', 'blocked-gap'], requests=1000,
                                          workers=4, repeats=self.a.repeats),
                        task2=dict(scenario='normal', workers=4, virtual_clients=4,
                                   duration_seconds=15, warmup=2048, repeats=self.a.repeats),
                        profile=dict(note='diagnostic only; never mixed into performance summaries',
                                     cpu_alloc_runs=3, io_runs=3),
                        variant_order='alternates by point and repeat')
        (self.local / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        self.log('setup complete: binaries uploaded and environments captured')

    # ---- one measured point --------------------------------------------
    def point(self, variant, tag, bench_args, summary, expect, server_extra=(), io=False):
        if tag in self.completed():
            return
        binary = self.remote + ('/server-new' if variant == 'optimized' else '/server-old')
        db = f'{self.remote}/{tag}.db'
        if self.ssh(self.a.server, f'test -e {db} && echo EXISTS || true').strip():
            raise RuntimeError(f'refusing to reuse {db}; a previous attempt was interrupted — inspect it')
        start = shlex.join([binary, '-mode', 'protected', '-addr', self.addr, '-db', db, *server_extra])
        pid = int(self.ssh(self.a.server, f'nohup {start} >{self.remote}/{tag}.server.log 2>&1 </dev/null & echo $!').strip())
        try:
            self.ssh(self.a.server, 'python3 -c ' + shlex.quote(
                "import socket,time\nfor i in range(200):\n try:\n  s=socket.create_connection(('%s',9000),.1);s.close();break\n"
                " except OSError:time.sleep(.05)\nelse:raise SystemExit('server readiness timeout')" % self.a.server_ip))
            if io:
                self.ssh(self.a.server, f'cat /proc/{pid}/io > {self.remote}/{tag}.io-before.txt')
            else:
                self.ssh(self.a.server, f'nohup sh {self.remote}/collect-server-metrics.sh {pid} {db} '
                                        f'{self.remote}/{tag}.metrics.csv >/dev/null 2>&1 </dev/null &')
            args = [self.remote + '/bench', '-addr', self.addr, '-implementation', 'protected',
                    '-repeats', '1', '-timeout', '5s', '-payload-bytes', '100',
                    '-connection-mode', 'persistent', '-seed', '452',
                    '-client-prefix', self.a.run_id + '-' + tag,
                    '-out', f'{self.remote}/{summary}-summary.csv',
                    '-latency-out', f'{self.remote}/{summary}-latency.csv',
                    '-timeline-out', f'{self.remote}/{summary}-timeline.csv', *bench_args]
            self.ssh(self.a.client, shlex.join(args), 900)
            if io:
                self.ssh(self.a.server, f'cat /proc/{pid}/io > {self.remote}/{tag}.io-after.txt')
        finally:
            # SIGTERM; with profiling flags the server flushes profiles and closes bbolt.
            self.ssh(self.a.server, f'if [ "$(readlink /proc/{pid}/exe)" = {shlex.quote(binary)} ]; then kill -TERM {pid}; fi; '
                                    f'for i in $(seq 1 200); do test ! -e /proc/{pid}/exe && exit 0; sleep 0.05; done; exit 1')
        self.download(self.a.client, f'{summary}-summary.csv')
        with (self.local / f'{summary}-summary.csv').open() as source:
            row = list(csv.DictReader(source))[-1]
        problems = [k for k in ('errors', 'attempt_errors', 'duplicate_effects') if int(row[k])]
        problems += expect(row)
        if problems:
            self.log(f'{tag}: INVALID {problems}: {row}')
            raise RuntimeError(f'{tag} failed validation; stopping so it can be documented')
        self.mark(tag)
        self.log(f'{tag}: {float(row["throughput_ops_s"]):.1f} ops/s p50={row["p50_ms"]}ms '
                 f'ack_p50={row["batch_ack_p50_ms"]}ms ack_max={row["batch_ack_max_ms"]}ms '
                 f'completions={row["completions_after"]} db={row["db_bytes_after"]}')

    def ordered(self, n):
        return VARIANTS if n % 2 == 0 else VARIANTS[::-1]

    def smoke(self):
        for variant in VARIANTS:
            self.point(variant, f'smoke-batch-{variant}', ['-scenario', 'batch-ack', '-requests', '40',
                       '-concurrency', '4', '-virtual-clients', '4', '-warmup', '0', '-verify-batch-ack'],
                       'smoke', lambda r: [] if int(r['completions_after']) == 4 else ['completions'])
            self.point(variant, f'smoke-normal-{variant}', ['-scenario', 'normal', '-requests', '200',
                       '-concurrency', '4', '-virtual-clients', '4', '-warmup', '16'],
                       'smoke', lambda r: [] if int(r['completions_after']) == 4 else ['completions'])
        self.log('smoke checks passed for both variants')

    def task1(self):
        self.log('task1: batch-ack B in {100,1000,10000} x W in {1,4}, fresh DB per point')
        n = 0
        for records in [100, 1000, 10000]:
            for workers in [1, 4]:
                for repeat in range(1, self.a.repeats + 1):
                    n += 1
                    for variant in self.ordered(n):
                        self.point(variant, f'batch-{variant}-b{records}-w{workers}-r{repeat}',
                                   ['-scenario', 'batch-ack', '-requests', str(records * workers),
                                    '-concurrency', str(workers), '-virtual-clients', str(workers),
                                    '-warmup', '0', '-repeat-start', str(repeat), '-verify-batch-ack'],
                                   f'task1-batch-{variant}',
                                   lambda r, w=workers: [] if int(r['completions_after']) == w else ['completions_after'])
        self.log('task1: ack-gc vs blocked-gap sanity comparison')
        for scenario in ['ack-gc', 'blocked-gap']:
            for repeat in range(1, self.a.repeats + 1):
                n += 1
                for variant in self.ordered(n):
                    def expect(r, s=scenario):
                        retained = int(r['completions_after']) - int(r['completions_before'])
                        # ack-gc keeps one record per client; blocked-gap must keep every result.
                        want = 4 if s == 'ack-gc' else int(r['successes'])
                        return [] if retained == want else [f'retained {retained} != {want}']
                    self.point(variant, f'{scenario}-{variant}-r{repeat}',
                               ['-scenario', scenario, '-requests', '1000', '-concurrency', '4',
                                '-virtual-clients', '4', '-warmup', '0', '-repeat-start', str(repeat)],
                               f'task1-{scenario}-{variant}', expect)
        self.log('task1 complete')

    def task2_args(self, repeat):
        return ['-scenario', 'normal', '-duration', '15s', '-concurrency', '4', '-virtual-clients', '4',
                '-warmup', '2048', '-repeat-start', str(repeat)]

    def task2(self):
        self.log('task2: reproduce 4 clients / 4 workers, 15s, warmup 2048 (uninstrumented)')
        for repeat in range(1, self.a.repeats + 1):
            for variant in self.ordered(repeat):
                self.point(variant, f'regress-{variant}-r{repeat}', self.task2_args(repeat),
                           f'task2-{variant}', lambda r: [] if int(r['completions_after']) == 4 else ['completions_after'])
        self.log('task2 reproduction complete; compare medians to 1967.6 (original) / 1645.4 (optimized) ops/s')

    def profile(self):
        self.log('profile: diagnostic runs, kept out of performance summaries')
        for repeat in range(1, 4):
            for variant in self.ordered(repeat):
                tag = f'prof-{variant}-r{repeat}'
                self.point(variant, tag, self.task2_args(repeat), f'diag-pprof-{variant}', lambda r: [],
                           server_extra=['-cpuprofile', f'{self.remote}/{tag}.cpu.pprof',
                                         '-memprofile', f'{self.remote}/{tag}.alloc.pprof'])
        for repeat in range(1, 4):
            for variant in self.ordered(repeat):
                self.point(variant, f'io-{variant}-r{repeat}', self.task2_args(repeat),
                           f'diag-io-{variant}', lambda r: [], io=True)
        self.log('profile complete; compare /proc io write_bytes and syscw per successful op')

    def collect(self):
        for host, role in [(self.a.server, 'server'), (self.a.client, 'client')]:
            archive = role + '-results.tar.gz'
            patterns = "*.csv *.server.log *.pprof *.txt" if role == 'server' else '*.csv'
            self.ssh(host, f'cd {self.remote} && tar -czf {archive} $(ls {patterns} 2>/dev/null)', 600)
            self.download(host, archive)
        self.run(['scp', *self.ssh_options, str(self.local / 'server-results.tar.gz'),
                  str(self.local / 'client-results.tar.gz'), str(self.local / 'manifest.json'),
                  self.a.controller + ':' + self.remote + '/'], 600)
        shutil.copy2(__file__, self.local / 'gc_followup.py')
        self.log('raw results downloaded and backed up on the controller')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--server', required=True, help='user@host of the server node')
    parser.add_argument('--client', required=True, help='user@host of the client node')
    parser.add_argument('--controller', required=True, help='user@host of the controller node')
    parser.add_argument('--server-ip', default='10.10.1.1', help='server private-network IP')
    parser.add_argument('--run-id', required=True, help='e.g. gcfollowup-20261112')
    parser.add_argument('--out', help='local results folder (default benchmark-results/<run-id>)')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('phases', nargs='+',
                        choices=['build', 'setup', 'smoke', 'task1', 'task2', 'profile', 'collect'])
    args = parser.parse_args()
    runner = Runner(args)
    runner.local.mkdir(parents=True, exist_ok=True)
    for phase in args.phases:
        getattr(runner, phase)()


if __name__ == '__main__':
    main()
