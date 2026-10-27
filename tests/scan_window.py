#!/usr/bin/env python3
"""Regress per-active-source windows and bounded read batches.

Requires the existing dedicated test instance with io_combine_limit=128.
Small cached fixtures verify geometry/results; SF100 measurements verify IO size.
"""
import json
import os
from pathlib import Path
import time

import memory_runtime as runtime
import pipeline_timeline as timeline
import scan_io_profile


OUT = timeline.OUT
SCHEMA = f'yaap_scan_window_{os.getpid()}'


def scan_records(data):
    for pipeline in data['yaap']['pipelines']:
        for worker in pipeline['workers']:
            io = worker.get('scan_io')
            if io and io.get('read_stream') != 'pg':
                yield pipeline['pipeline_id'], io


def run_case(name, query, workers, window, batch_cap, expected_batch,
             expected_sources=1, statements=1):
    expected = timeline.checked('SET pg_yaap.enabled=off;' + query)
    prefix = OUT / f'{name}-{time.time_ns()}'
    setup = timeline.setup(prefix, workers=workers, mode='trace',
                           read_stream='yaap', io_mode='detail')
    setup += (f'SET pg_yaap.scan_prefetch_buffers={window}; '
              f'SET pg_yaap.scan_prefetch_batch_pages={batch_cap}; '
              'SET pg_yaap.scan_prefetch_requests=1; ')
    result = timeline.run(setup + query)
    (OUT / f'{name}.out').write_text(result.stdout)
    (OUT / f'{name}.err').write_text(result.stderr)
    assert result.returncode == 0, (name, result.stderr)
    assert result.stdout.splitlines() == expected.splitlines(), name
    files = sorted(OUT.glob(prefix.name + '.*.json'))
    assert len(files) == statements, (name, files)
    traces = sorted((json.loads(f.read_text()) for f in files),
                    key=lambda d: d['yaap']['query_origin_ns'])
    geometries = []
    for data in traces:
        # Reuse publication, histogram and task-timeline invariants.
        scan_io_profile.validate_io(data)
        for pipeline_id, io in scan_records(data):
            config = io['config']
            actual_window = config['granted_pages']
            actual_batch = config['batch_pages']
            assert config['request_cap'] == 1, (name, config)
            assert actual_batch == expected_batch, (name, config)
            assert actual_window >= 2 * actual_batch, (name, config)
            assert actual_window <= config['pg_pin_cap'], (name, config)
            assert actual_window <= window, (name, config)
            geometries.append(dict(pipeline=pipeline_id, window=actual_window,
                                   batch=actual_batch))
    final = traces[-1]
    active = {pipeline_id for pipeline_id, io in scan_records(final)
              if io['observed']['consumed_pages']}
    if expected_sources:
        assert len(active) == expected_sources, (name, active)
    else:
        assert not active, (name, active)
    assert geometries, (name, 'no initialized scan records')
    return {'case': name, 'pass': True, 'traces': [str(f) for f in files],
            'geometries': geometries}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    combine = timeline.checked("SET pg_yaap.enabled=off; SELECT setting "
                               "FROM pg_settings WHERE name='io_combine_limit';")
    assert combine.strip() == '128', ('test requires server io_combine_limit=128', combine)
    table = f'{SCHEMA}.a'
    runtime.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {table} AS SELECT '
                "i::bigint AS id,repeat('x',128)::text AS padding "
                'FROM generate_series(1,60000) i; '
                f'CREATE TABLE {SCHEMA}.empty AS SELECT * FROM {table} WHERE false; '
                f'ANALYZE {table}; ANALYZE {SCHEMA}.empty;')
    aliases = ','.join(f'{table} t{i}' for i in range(6))
    predicates = ' AND '.join(f't{i}.id=t0.id' for i in range(1, 6))
    multi = f'SELECT count(*),sum(t0.id) FROM {aliases} WHERE {predicates};'
    single = f'SELECT count(*),sum(id) FROM {table};'
    results = []
    try:
        cases = [
            ('multi-w1', multi, 1, 8192, 128, 128, 6, 1),
            ('multi-w4', multi, 4, 8192, 128, 128, 6, 1),
            ('single-w4', single, 4, 8192, 128, 128, 1, 1),
            ('small-window', multi, 4, 17, 128, 8, 6, 1),
            ('explicit-batch-cap', multi, 4, 8192, 16, 16, 6, 1),
            ('limit-same-backend', f'SELECT 1 FROM {table} LIMIT 7;' + single,
             4, 17, 128, 8, 1, 2),
            ('empty', f'SELECT id FROM {SCHEMA}.empty;', 4, 8192, 128, 128, 0, 1),
        ]
        for args in cases:
            results.append(run_case(*args))
            (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
            print(json.dumps(results[-1]), flush=True)
    finally:
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
