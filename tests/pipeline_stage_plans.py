#!/usr/bin/env python3
"""Compare sink stage plans with PostgreSQL on an isolated test instance.

Uses the connection and module environment variables from memory_runtime.py.
Includes single-worker and partitioned plans so each
producer's local state must be included exactly once at the combine barrier.
"""
import os
import re

import memory_runtime as runtime

SCHEMA = f'yaap_stage_plan_test_{os.getpid()}'


def main():
    runtime.ddl(
        f'CREATE SCHEMA {SCHEMA}; '
        f'CREATE TABLE {SCHEMA}.a AS SELECT i::bigint id,(i%97)::int k, '
        "(CASE WHEN i%3=0 THEN 'A' ELSE 'B' END)::char(1) AS ck "
        'FROM generate_series(1,40000) i; '
        f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {SCHEMA}.a WHERE id%3=0; '
        f'CREATE TABLE {SCHEMA}.perfect AS SELECT i::bigint id, '
        "(CASE WHEN i%3=0 THEN 'A' ELSE 'B' END)::char(1) ck "
        'FROM generate_series(1,1000000) i; '
        f'CREATE TABLE {SCHEMA}.tiny AS SELECT i::bigint id FROM generate_series(1,3) i; '
        f'ANALYZE {SCHEMA}.a; ANALYZE {SCHEMA}.b; ANALYZE {SCHEMA}.tiny; ANALYZE {SCHEMA}.perfect;')
    a, b, tiny = (f'{SCHEMA}.{name}' for name in ('a', 'b', 'tiny'))
    queries = {
        'perfect-aggregate': f'SELECT ck,sum(id),count(*) FROM {SCHEMA}.perfect GROUP BY ck ORDER BY ck;',
        'partitioned-aggregate': f'SELECT id,sum(k),count(*) FROM {a} GROUP BY id ORDER BY id;',
        'join-build': f'SELECT a.k,count(*),sum(a.id+b.id) FROM {a} a JOIN {b} b '
                      'ON a.id=b.id GROUP BY a.k ORDER BY a.k;',
        'cross-product-build': f'SELECT count(*),sum(a.id+b.id) FROM {a} a CROSS JOIN {tiny} b;',
        'independent-builds': f'SELECT l.k,l.v,r.v FROM '
                              f'(SELECT k,sum(id) v FROM {a} GROUP BY k) l JOIN '
                              f'(SELECT k,sum(id) v FROM {b} GROUP BY k) r ON l.k=r.k ORDER BY l.k;',
    }
    try:
        for name, query in queries.items():
            expected = runtime.checked('SET pg_yaap.enabled=off; ' + query)
            for workers in (1, 4):
                for partition_probe in ('off', 'on'):
                    settings = runtime.setup(workers, '256MB') + (
                        f'SET pg_yaap.hash_join_partition_probe={partition_probe}; ')
                    if name == 'perfect-aggregate':
                        settings += 'SET pg_yaap.profile=on; '
                    result = runtime.run(settings + query)
                    tag = f'{name}-w{workers}-probe-{partition_probe}'
                    if result.returncode or result.stdout != expected:
                        (runtime.OUT / f'{tag}.expected').write_text(expected)
                        (runtime.OUT / f'{tag}.out').write_text(result.stdout)
                        (runtime.OUT / f'{tag}.err').write_text(result.stderr)
                        raise AssertionError(f'{tag}: result mismatch or SQL error; see {runtime.OUT}')
                    if name == 'perfect-aggregate':
                        # Small integer domains still use regular hashagg.
                        # char(1) keys select the actual perfect-hash sink;
                        # require profiler evidence to keep this regression
                        # from silently losing coverage as planning changes.
                        if not re.search(r'overall stage=perfect_agg_combine_global[^\n]*calls=1\b',
                                         result.stderr):
                            raise AssertionError(f'{tag}: perfect-hash combine was not executed once')
                        updates = re.findall(
                            r'role=(leader|worker) worker_index=(-?\d+)[^\n]*'
                            r'stage=perfect_agg_update_local[^\n]*rows=(\d+)', result.stderr)
                        producer_count = sum(int(rows) > 0 for _, _, rows in updates)
                        # One idle process may legitimately claim several complete RUN tasks.
                        required = 1
                        if producer_count < required:
                            raise AssertionError(f'{tag}: expected {required} active perfect-hash producers, '
                                                 f'observed {producer_count}')
                        if sum(int(rows) for _, _, rows in updates) != 1000000:
                            raise AssertionError(f'{tag}: perfect-hash producers did not consume every row')
                        if any(role == 'leader' for role, _, _ in updates):
                            raise AssertionError(f'{tag}: leader unexpectedly ran the sink')
                    print(f'{tag}: PASS', flush=True)
    finally:
        runtime.ddl(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
