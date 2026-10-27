#!/usr/bin/env python3
"""Scan cost attribution checks; reuse the dedicated timeline test connection."""
import collections
import json
import os
import re
from pathlib import Path
import time

import pipeline_timeline as timeline

OUT = timeline.OUT
SCHEMA = f'yaap_scan_profile_{os.getpid()}'
READ_STREAM = os.environ.get('YAAP_TEST_READ_STREAM', 'pg')
COSTS = ('source_ns', 'pg_access_ns', 'row_to_column_ns', 'predicate_ns',
         'setup_ns', 'visible_rows', 'output_rows', 'samples')


def check_trace(data, mode, dropped=False):
    timeline.validate(data, dropped)
    totals = collections.Counter()
    for event in data['traceEvents']:
        if event['ph'] != 'X':
            continue
        scan = event['args'].get('scan_cost')
        if not scan:
            continue
        assert mode != 'off'
        assert scan['mode'] == mode
        assert scan['tuple_times_estimated'] == (mode == 'sample')
        assert all(scan[k] >= 0 for k in COSTS)
        assert scan['source_ns'] <= event['dur'] * 1000 + 2000
        for k in COSTS:
            totals[(event['args']['pipeline_id'], event['tid'], k)] += scan[k]
    found = []
    for p in data['yaap']['pipelines']:
        for w in p['workers']:
            scan = w.get('scan_cost')
            if not scan:
                continue
            assert mode != 'off'
            if scan['source_ns']:
                found.append(scan)
            if not dropped:
                for k in COSTS:
                    assert totals[(p['pipeline_id'], w['slot_id'], k)] == scan[k], (k, w)
    if mode != 'off':
        assert found, 'missing scan cost records'
    return found


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    timeline.SCHEMA = SCHEMA
    table = f'{SCHEMA}.a'
    timeline.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {table} AS SELECT '
                 'i::bigint AS id,(i%17)::int AS k,'
                 '(i%100)::numeric(15,2) AS n,'
                 "(repeat('x',40)||(i%17)::text)::text AS s "
                 'FROM generate_series(1,60000) i; '
                 f'CREATE TABLE {SCHEMA}.empty AS SELECT * FROM {table} WHERE false; '
                 f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {table} WHERE id<20000; '
                 f'ANALYZE {table}; ANALYZE {SCHEMA}.empty; ANALYZE {SCHEMA}.b;')
    queries = {
        'scan': f'SELECT sum(id),count(*) FROM {table};',
        'filter': f'SELECT k,sum(id),sum(n) FROM {table} WHERE id<30000 GROUP BY k ORDER BY k;',
        'strings': f'SELECT s,count(*) FROM {table} WHERE id<1000 GROUP BY s ORDER BY s;',
        'reject-all': f'SELECT id FROM {table} WHERE id<0;',
        'empty': f'SELECT id FROM {SCHEMA}.empty;',
        'two-scans': f'SELECT a.k,count(*) FROM {table} a JOIN {table} b ON a.id=b.id GROUP BY a.k ORDER BY a.k;',
    }
    try:
        cases = [('off', 'scan', 4, 'off', False, True, 8192),
                 ('sample-single', 'scan', 1, 'sample', False, True, 8192),
                 ('sample-multi', 'filter', 4, 'sample', False, True, 8192),
                 ('exact', 'filter', 4, 'exact', False, True, 8192),
                 ('strings', 'strings', 4, 'exact', False, True, 8192),
                 ('jit-off', 'filter', 4, 'sample', False, False, 8192),
                 ('reject-all', 'reject-all', 4, 'exact', False, True, 8192),
                 ('empty', 'empty', 4, 'sample', False, True, 8192),
                 ('two-scans', 'two-scans', 4, 'sample', False, True, 8192),
                 ('legacy', 'filter', 4, 'off', True, True, 8192),
                 ('both', 'filter', 4, 'sample', True, True, 8192),
                 ('overflow', 'filter', 4, 'sample', False, True, 1)]
        for name, query_name, workers, mode, legacy, jit, capacity in cases:
            query = queries[query_name]
            expected = timeline.checked('SET pg_yaap.enabled=off;' + query)
            prefix = OUT / f'{name}-{time.time_ns()}'
            config = timeline.setup(prefix, workers, 'trace', capacity, legacy, read_stream=READ_STREAM)
            config += f'SET pg_yaap.profile_scan={mode}; SET pg_yaap.jit_deform={"on" if jit else "off"}; '
            result = timeline.run(config + query)
            (OUT / f'{name}.expected').write_text(expected)
            (OUT / f'{name}.out').write_text(result.stdout)
            (OUT / f'{name}.err').write_text(result.stderr)
            assert result.returncode == 0, (name, result.stderr)
            assert sorted(result.stdout.splitlines()) == sorted(expected.splitlines()), name
            files = list(OUT.glob(prefix.name + '.*.json'))
            assert len(files) == 1, name
            data = json.loads(files[0].read_text())
            scans = check_trace(data, mode, capacity == 1)
            if mode != 'off':
                assert 'pg_yaap[scan]' in result.stderr
                for line in result.stderr.splitlines():
                    if 'pg_yaap[scan] worker ' not in line:
                        continue
                    values = dict(re.findall(r'(\w+)=([^\s]+)', line))
                    ns = {k: float(values[k]) for k in (
                        'source_ms', 'pg_access_ms', 'row_to_column_ms',
                        'predicate_ms', 'setup_ms', 'load_ms', 'prepare_ms',
                        'tuple_ms', 'qual_ms', 'proj_ms', 'overestimate_ms')}
                    assert abs(ns['pg_access_ms'] - ns['load_ms'] - ns['prepare_ms'] - ns['tuple_ms']) < .004
                    assert abs(ns['row_to_column_ms'] - ns['qual_ms'] - ns['proj_ms']) < .003
                    if mode == 'exact':
                        assert ns['pg_access_ms'] + ns['row_to_column_ms'] + ns['predicate_ms'] + ns['setup_ms'] <= ns['source_ms'] + .004
                        assert ns['overestimate_ms'] == 0
                if query_name == 'two-scans':
                    ids = {(p['pipeline_id'], w['scan_cost']['operator_id'], w['scan_cost']['relation_oid'])
                           for p in data['yaap']['pipelines'] for w in p['workers']
                           if w.get('scan_cost', {}).get('source_ns')}
                    assert len(ids) >= 2 and len({i[2] for i in ids}) == 1
                    assert all(i[1] != 4294967295 for i in ids)
                if query_name == 'scan':
                    assert sum(s['visible_rows'] for s in scans) == 60000
                    assert sum(s['output_rows'] for s in scans) == 60000
                if query_name == 'filter':
                    assert sum(s['visible_rows'] for s in scans) == 60000
                    assert sum(s['output_rows'] for s in scans) == 29999
                if query_name == 'empty':
                    assert sum(s['visible_rows'] for s in scans) == 0
            elif not legacy:
                assert 'pg_yaap[scan]' not in result.stderr
            results.append({'case': name, 'pass': True, 'trace': str(files[0])})
            (OUT / 'summary.json').write_text(json.dumps(results, indent=2))
            print(json.dumps(results[-1]), flush=True)
        # Independent scan reporting must work without timeline or legacy modes.
        result = timeline.run(timeline.setup(OUT/'standalone', mode='off', read_stream=READ_STREAM) +
                              'SET pg_yaap.profile_scan=sample;' + queries['scan'])
        (OUT/'standalone.err').write_text(result.stderr)
        assert result.returncode == 0 and 'pg_yaap[scan]' in result.stderr
        results.append({'case': 'standalone', 'pass': True})
        timeline.abnormal_checks(results, queries['two-scans'], scan_mode='sample', read_stream=READ_STREAM)
    finally:
        timeline.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT/'summary.json').write_text(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
