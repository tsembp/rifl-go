import csv
from pathlib import Path
import statistics

root = Path('/home/tsemb_p/Documents/ChatGPT/452/benchmark-results/gc-20261111-QtN7Qx')
groups = {}
total = 0
for variant in ['baseline', 'original', 'optimized']:
    with (root/(variant+'-summary.csv')).open() as source:
        rows = list(csv.DictReader(source))
    for row in rows:
        if any(int(row[key]) for key in ['errors','attempt_errors','duplicate_effects']):
            raise RuntimeError('Invalid measurement: '+str(row))
        if row['connection_mode'] != 'persistent' or int(row['concurrency']) != 4:
            raise RuntimeError('Unexpected benchmark configuration')
        groups.setdefault((int(row['virtual_clients']),variant),[]).append(row)
        total += 1

lines = ['# Client-count comparison: old versus optimized GC', '',
         'Three CloudLab d430 nodes, Ubuntu 24.04; server and client on the private LAN.',
         'Five independent repetitions per point, 15 seconds each, four workers, 100-byte payload,',
         'persistent TCP connections, warmup 2048, default synchronous bbolt durability.',
         'Every repetition starts a fresh server process and database. Variant order rotates.', '',
         'Baseline and original protected binaries come from commit 6a50b38. Optimized code is',
         'the uncommitted source snapshot archived with this run. Compiler and hashes are in manifest.json.', '',
         '| Clients | Baseline ops/s | Old protected ops/s | Optimized ops/s | Optimized / old | Old p99 ms | Optimized p99 ms |',
         '|---:|---:|---:|---:|---:|---:|---:|']
summary = []
for clients in [4,16,64,256,1024]:
    medians = {}
    for variant in ['baseline','original','optimized']:
        rows = groups[(clients,variant)]
        if len(rows) != 5:
            raise RuntimeError(f'Expected five repeats for {clients}/{variant}, got {len(rows)}')
        throughput = [float(row['throughput_ops_s']) for row in rows]
        latency = [float(row['p99_ms']) for row in rows]
        values = dict(clients=clients,variant=variant,median_ops_s=statistics.median(throughput),
                      min_ops_s=min(throughput),max_ops_s=max(throughput),median_p99_ms=statistics.median(latency),
                      min_p99_ms=min(latency),max_p99_ms=max(latency),repeats=5)
        medians[variant] = values
        summary.append(values)
    base,old,new = [medians[v] for v in ['baseline','original','optimized']]
    lines.append(f'| {clients} | {base["median_ops_s"]:,.1f} | {old["median_ops_s"]:,.1f} | '
                 f'{new["median_ops_s"]:,.1f} | {new["median_ops_s"]/old["median_ops_s"]:.2f}x | '
                 f'{old["median_p99_ms"]:.3f} | {new["median_p99_ms"]:.3f} |')
lines += ['', f'All {total} measurement points had zero reported logical errors, attempt errors, and duplicate effects.',
          'Values above are medians across repetitions; min/max ranges are in comparison.csv.', '',
          'These results compare durable TCP RPC implementations. They are not CXL or failover measurements.',
          'The warmup differs from the earlier reported runs (2048 instead of 1000), so use this paired',
          'old/new comparison rather than attributing differences from the old report solely to GC.',
          'The raw-data archives contain samples, timelines, server logs, and process metrics.', '']
(root/'comparison.md').write_text('\n'.join(lines))
with (root/'comparison.csv').open('w',newline='') as output:
    writer = csv.DictWriter(output,fieldnames=list(summary[0]))
    writer.writeheader()
    writer.writerows(summary)
print('\n'.join(lines))
