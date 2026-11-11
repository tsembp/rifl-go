import csv
import io
import json
import math
from pathlib import Path
import tarfile

root = Path('/home/tsemb_p/Documents/ChatGPT/452/benchmark-results/gc-20261111-QtN7Qx')
checks = []
with tarfile.open(root/'client-results.tar.gz', 'r:gz') as archive:
    for variant in ['baseline','original','optimized']:
        with (root/(variant+'-summary.csv')).open() as source:
            summary = {(int(r['virtual_clients']), int(r['repeat'])): r for r in csv.DictReader(source)}
        samples = {}
        with io.TextIOWrapper(archive.extractfile(variant+'-latency.csv')) as source:
            for row in csv.DictReader(source):
                if row['success'] != 'true' or row['kind'] != 'operation':
                    raise RuntimeError('Unexpected failed or non-operation raw sample: '+str(row))
                key = (int(row['virtual_clients']),int(row['repeat']))
                samples.setdefault(key,[]).append(int(row['latency_ns']))
        if set(samples) != set(summary):
            raise RuntimeError('Raw data does not cover summary groups')
        for key, values in samples.items():
            row = summary[key]
            if len(values) != int(row['requests']) or len(values) != int(row['successes']):
                raise RuntimeError('Raw sample count mismatch')
            values.sort()
            expected = values[math.ceil(.99*len(values))-1]
            actual = round(float(row['p99_ms'])*1e6)
            if abs(expected-actual)>1:
                raise RuntimeError(f'p99 mismatch: {variant}/{key}: {expected} vs {actual}')
        checks.append(dict(variant=variant,points=len(summary),raw_samples=sum(map(len,samples.values())),
                           sample_counts_match=True,p99_recomputed_from_raw=True))
(root/'raw-verification.json').write_text(json.dumps(checks,indent=2)+'\n')
print(json.dumps(checks,indent=2))
