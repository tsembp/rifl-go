"""Validate raw samples and summarize the fixed-client concurrency experiment."""
import csv
import io
import json
import math
from pathlib import Path
import statistics
import tarfile

ROOT = Path('/home/tsemb_p/Documents/ChatGPT/452/benchmark-results/concurrency-20261111-qwUdyA')
variants = ['baseline','original','optimized']
groups = {}
verification = []
with tarfile.open(ROOT/'client-results.tar.gz','r:gz') as archive:
    for variant in variants:
        with (ROOT/(variant+'-summary.csv')).open() as source:
            rows = list(csv.DictReader(source))
        if len(rows)!=25:
            raise RuntimeError('Expected 25 points for '+variant)
        summaries = {}
        for row in rows:
            if any(int(row[k]) for k in ['errors','attempt_errors','duplicate_effects']):
                raise RuntimeError('Invalid measurement: '+str(row))
            if int(row['virtual_clients'])!=64 or row['connection_mode']!='persistent':
                raise RuntimeError('Unexpected workload configuration')
            key = (int(row['concurrency']),int(row['repeat']))
            if key in summaries:
                raise RuntimeError('Repeated experiment identifier')
            summaries[key] = row
            groups.setdefault((key[0],variant),[]).append(row)
        samples = {}
        with io.TextIOWrapper(archive.extractfile(variant+'-latency.csv')) as source:
            for row in csv.DictReader(source):
                if row['success']!='true' or row['kind']!='operation':
                    raise RuntimeError('Failed or unexpected raw sample')
                key = (int(row['concurrency']),int(row['repeat']))
                samples.setdefault(key,[]).append(int(row['latency_ns']))
        if set(samples)!=set(summaries):
            raise RuntimeError('Raw sample groups do not match summaries')
        for key,values in samples.items():
            row = summaries[key]
            if len(values)!=int(row['successes']) or len(values)!=int(row['requests']):
                raise RuntimeError('Raw sample count mismatch')
            values.sort()
            p99 = values[math.ceil(.99*len(values))-1]
            if abs(p99-round(float(row['p99_ms'])*1e6))>1:
                raise RuntimeError('Raw p99 does not match summary')
        verification.append(dict(variant=variant,points=25,raw_samples=sum(map(len,samples.values())),
                                 sample_counts_match=True,p99_recomputed_from_raw=True))

summary = []
lines = ['# Concurrency sweep with 64 fixed logical clients','',
         'Server pc713, client pc708, controller pc705; CloudLab d430, Ubuntu 24.04.',
         'Five independent 15-second repetitions per point; 64 logical clients; workers varied.',
         '100-byte payload, 2048 warmup requests, persistent TCP per worker, synchronous bbolt.',
         'Fresh server process and database per point; variant order rotates. Binaries match',
         'the preceding client-count sweep exactly (hashes and source snapshots are included).','',
         '| Workers | Baseline ops/s | Old protected ops/s | Optimized ops/s | Optimized / old | Baseline p99 ms | Old p99 ms | Optimized p99 ms |',
         '|---:|---:|---:|---:|---:|---:|---:|---:|']
for workers in [1,4,16,32,64]:
    med = {}
    for variant in variants:
        rows = groups[(workers,variant)]
        if len(rows)!=5:
            raise RuntimeError('Expected five repetitions')
        rates = [float(row['throughput_ops_s']) for row in rows]
        tails = [float(row['p99_ms']) for row in rows]
        data = dict(workers=workers,clients=64,variant=variant,repeats=5,
                    median_ops_s=statistics.median(rates),min_ops_s=min(rates),max_ops_s=max(rates),
                    median_p99_ms=statistics.median(tails),min_p99_ms=min(tails),max_p99_ms=max(tails))
        med[variant]=data
        summary.append(data)
    base,old,new = [med[v] for v in variants]
    lines.append(f'| {workers} | {base["median_ops_s"]:,.1f} | {old["median_ops_s"]:,.1f} | '
                 f'{new["median_ops_s"]:,.1f} | {new["median_ops_s"]/old["median_ops_s"]:.2f}x | '
                 f'{base["median_p99_ms"]:.3f} | {old["median_p99_ms"]:.3f} | {new["median_p99_ms"]:.3f} |')
lines += ['', 'All 75 points had zero reported logical errors, attempt errors, and duplicate effects.',
          f'Independent raw-data checks verified counts and recomputed p99 for {sum(v["raw_samples"] for v in verification):,} logical requests.',
          'Tables show medians across repetitions; min/max ranges are in comparison.csv.', '',
          'This is a closed-loop benchmark: each worker waits for its reply before sending again.',
          'Increasing workers tests concurrent request pressure while client metadata stays fixed.',
          'The single bbolt writer can saturate: more workers may then add queueing latency rather',
          'than throughput. Measurements alone do not identify every component of that latency.',
          'These are TCP/bbolt results, not CXL, power-loss, or cross-server failover measurements.', '']
manifest = json.loads((ROOT/'manifest.json').read_text())
if manifest.get('interruptions'):
    lines += ['The controller SSH connection stalled between points after 25 valid measurements.',
              'The server was confirmed idle and the next database absent. The remaining points',
              'resumed with fresh SSH control sockets and unchanged binaries/settings; completed',
              'points were retained. This was not an in-flight benchmark RPC failure. See manifest.json.', '']
variable = [r for r in summary if r['min_ops_s'] < .8*r['median_ops_s'] or r['max_ops_s'] > 1.2*r['median_ops_s']]
if variable:
    lines += ['## Repetition variability','',
              'The following groups contain a repetition more than 20% from their median.',
              'Those points are retained, not discarded. These are descriptive medians, not',
              'confidence intervals; additional repetitions would help characterize variability.','']
    for row in variable:
        lines.append(f'- {row["variant"]}, {row["workers"]} workers: {row["min_ops_s"]:.1f}–{row["max_ops_s"]:.1f} ops/s; median {row["median_ops_s"]:.1f}.')
    lines += ['']
prior = ROOT.parent/'gc-20261111-QtN7Qx'/'comparison.csv'
with prior.open() as source:
    earlier = {r['variant']:r for r in csv.DictReader(source) if int(r['clients'])==64}
lines += ['## Repeated point from the first sweep','',
          'Both sweeps include four workers and 64 clients, with the same binaries and settings.',
          'This provides a cross-run reproducibility check; it is not a confidence interval.','',
          '| Variant | First sweep ops/s | This sweep ops/s | Difference |',
          '|---|---:|---:|---:|']
for data in summary:
    if data['workers']==4:
        before = float(earlier[data['variant']]['median_ops_s'])
        after = data['median_ops_s']
        lines.append(f'| {data["variant"]} | {before:,.1f} | {after:,.1f} | {(after/before-1)*100:+.1f}% |')
lines += ['']
(ROOT/'comparison.md').write_text('\n'.join(lines))
(ROOT/'raw-verification.json').write_text(json.dumps(verification,indent=2)+'\n')
with (ROOT/'comparison.csv').open('w',newline='') as output:
    writer = csv.DictWriter(output,fieldnames=list(summary[0]))
    writer.writeheader()
    writer.writerows(summary)
print('\n'.join(lines))
