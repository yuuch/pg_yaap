#!/usr/bin/env python3
"""Exercise shared-memory execution and recovery against PostgreSQL results.

Requires a dedicated running PostgreSQL instance. PGHOST/PGPORT/PGUSER/PGDATABASE,
PSQL and YAAP_LIBRARY select it; YAAP_TEST_MEMORY overrides the small-budget
regression setting (default 64MB). The fixture schema is dropped on completion.
"""
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
PSQL = os.environ.get('PSQL', '/Users/chenyunwen/proj/postgres/installed-bench/bin/psql')
LIBRARY = Path(os.environ.get('YAAP_LIBRARY', ROOT / 'build-bench/pg_yaap.dylib')).resolve()
BASE = [PSQL, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-P', 'pager=off']
SCHEMA = f'yaap_memory_test_{os.getpid()}'
OUT = Path(os.environ.get('YAAP_TEST_OUTPUT', ROOT / 'artifacts/memory-fixes-20261019/regression'))
OUT.mkdir(parents=True, exist_ok=True)
TEST_MEMORY = os.environ.get('YAAP_TEST_MEMORY', '64MB')


def run(sql, timeout=90):
    return subprocess.run(BASE, input=sql, text=True, capture_output=True, timeout=timeout)


def checked(sql):
    result = run(sql)
    for _ in range(30):
        if result.returncode == 0 or 'recovery mode' not in result.stderr:
            break
        time.sleep(0.2)
        result = run(sql)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def ddl(sql):
    # The existing TPC-H database has unrelated pg_duckdb DDL event hooks.
    # Preserve their prior state and isolate fixture changes transactionally.
    triggers = checked("SET pg_yaap.enabled=off; SELECT evtname||':'||evtenabled::text "
                       "FROM pg_event_trigger WHERE evtname LIKE 'duckdb_%';")
    disable, restore = [], []
    for line in triggers.splitlines():
        name, state = line.rsplit(':', 1)
        quoted = '"' + name.replace('"', '""') + '"'
        disable.append(f'ALTER EVENT TRIGGER {quoted} DISABLE;')
        action = {'O': 'ENABLE', 'D': 'DISABLE', 'R': 'ENABLE REPLICA', 'A': 'ENABLE ALWAYS'}[state]
        restore.append(f'ALTER EVENT TRIGGER {quoted} {action};')
    return checked('SET pg_yaap.enabled=off; BEGIN; ' + ''.join(disable)
                   + sql + ''.join(restore) + 'COMMIT;')


def setup(workers=12, memory='64MB'):
    return (f"LOAD '{str(LIBRARY).replace(chr(39), chr(39)*2)}'; "
            f"SET pg_yaap.enabled=on; SET pg_yaap.parallel=on; "
            f"SET pg_yaap.parallel_max_workers={workers}; "
            f"SET pg_yaap.memory_limit='{memory}'; "
            "SET pg_yaap.parallel_min_relation_blocks=0; "
            "SET statement_timeout='60s'; ")


def main():
    results = []
    ddl(f'CREATE SCHEMA {SCHEMA}; '
        f'CREATE TABLE {SCHEMA}.a AS SELECT i::bigint AS id,(i%97)::int AS k,'
        "lpad(i::text,80,'x')::text AS s FROM generate_series(1,60000) i; "
        f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {SCHEMA}.a WHERE id%3=0; '
        f'ANALYZE {SCHEMA}.a; ANALYZE {SCHEMA}.b; '
        f"CREATE FUNCTION {SCHEMA}.segment_selftest() RETURNS boolean AS "
        f"'{LIBRARY}','pg_yaap_segment_selftest' LANGUAGE C;")
    try:
        selftest = checked(f'SET pg_yaap.enabled=off; SELECT {SCHEMA}.segment_selftest();')
        if selftest != 't\n':
            raise AssertionError('segment lifecycle selftest failed')
        results.append({'case': 'segment-selftest', 'pass': True})
        queries = {
            'aggregate': f'SELECT k,sum(id),count(*) FROM {SCHEMA}.a GROUP BY k ORDER BY k;',
            'join': f'SELECT a.k,sum(a.id+b.id) FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.id=b.id GROUP BY a.k ORDER BY a.k;',
            'order_strings': f'SELECT id,s,k FROM {SCHEMA}.a ORDER BY s DESC,id LIMIT 1000;',
            'join_resume': f'SELECT a.id,b.id,b.s FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.k=b.k WHERE a.id<100 ORDER BY a.id,b.id;',
            'join_multikey': f'SELECT a.id,b.s FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.k=b.k AND a.id=b.id ORDER BY a.id;',
            'join_semi_residual': f'SELECT a.id FROM {SCHEMA}.a a WHERE a.id<500 AND EXISTS (SELECT 1 FROM {SCHEMA}.b b WHERE b.k=a.k AND b.id>a.id) ORDER BY a.id;',
            'join_anti_residual': f'SELECT a.id FROM {SCHEMA}.a a WHERE a.id<500 AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.b b WHERE b.k=a.k AND b.id<a.id) ORDER BY a.id;',
            'join_left_residual': f'SELECT a.id FROM {SCHEMA}.a a LEFT JOIN {SCHEMA}.b b ON a.k=b.k AND b.id<a.id WHERE a.id<500 ORDER BY a.id;',
            'join_independent_branches': (
                f'SELECT l.k,l.v,r.v FROM '
                f'(SELECT a.k,sum(a.id) AS v FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.id=b.id GROUP BY a.k) l '
                f'JOIN (SELECT a.k,sum(b.id) AS v FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.id=b.id GROUP BY a.k) r '
                'ON l.k=r.k ORDER BY l.k;'),
        }
        for name, query in queries.items():
            expected = checked('SET pg_yaap.enabled=off; ' + query)
            for workers, memory in ((4, TEST_MEMORY), (12, TEST_MEMORY), (12, '8GB')):
                tag = f'{name}-w{workers}-{memory}'
                start = time.monotonic()
                result = run(setup(workers, memory) + query)
                (OUT / f'{tag}.out').write_text(result.stdout)
                (OUT / f'{tag}.err').write_text(result.stderr)
                (OUT / f'{tag}.expected').write_text(expected)
                ok = result.returncode == 0 and result.stdout == expected
                row = {'case': tag, 'pass': ok, 'seconds': round(time.monotonic()-start, 3)}
                results.append(row)
                print(json.dumps(row), flush=True)
                if not ok:
                    raise AssertionError(f'{tag}: output mismatch or SQL error; see {OUT}')
        # Force pairs larger than their soft admission budget. They must
        # serialize with bounded segment windows and preserve resumed strings.
        query = queries['join_resume']
        expected = checked('SET pg_yaap.enabled=off; ' + query)
        result = run(setup(4, '64MB') +
                     "SET pg_yaap.memory_soft_limit='1MB'; SET pg_yaap.profile=on; " + query)
        (OUT / 'oversized-pair.out').write_text(result.stdout)
        (OUT / 'oversized-pair.err').write_text(result.stderr)
        ok = (result.returncode == 0 and result.stdout == expected
              and 'hash_join_oversized_partition_pairs' in result.stderr)
        results.append({'case': 'oversized-pair', 'pass': ok})
        if not ok:
            raise AssertionError('oversized partition pair regression failed')
        # A parallel consumer must keep workers alive when a pending pair
        # temporarily cannot be admitted. Unlike the output sink above, this
        # aggregate permits multiple source workers to compete for the budget.
        query = (f'SELECT a.k,sum(a.id+b.id) FROM {SCHEMA}.a a '
                 f'JOIN {SCHEMA}.b b ON a.k=b.k WHERE a.id<500 '
                 'GROUP BY a.k ORDER BY a.k;')
        expected = checked('SET pg_yaap.enabled=off; ' + query)
        result = run(setup(4, '256MB') +
                     "SET pg_yaap.memory_soft_limit='1MB'; SET pg_yaap.profile=on; " + query)
        (OUT / 'pair-admission-wait.out').write_text(result.stdout)
        (OUT / 'pair-admission-wait.err').write_text(result.stderr)
        ok = (result.returncode == 0 and result.stdout == expected
              and 'hash_join_pair_wait' in result.stderr)
        results.append({'case': 'pair-admission-wait', 'pass': ok})
        if not ok:
            raise AssertionError('parallel partition admission wait regression failed')
        # Cancellation must also interrupt workers sleeping on admission,
        # then leave the same backend usable for another YAAP query.
        slow_query = query.replace('a.id<500 ', 'a.id<50000 ')
        recovery_query = queries['aggregate']
        sql = (setup(4, '256MB') + "SET pg_yaap.memory_soft_limit='1MB'; "
               '\n\\set ON_ERROR_STOP off\n'
               "SET statement_timeout='80ms'; " + slow_query +
               " SET statement_timeout='60s';\n\\set ON_ERROR_STOP on\n" + recovery_query)
        result = run(sql)
        (OUT / 'pair-admission-cancel.out').write_text(result.stdout)
        (OUT / 'pair-admission-cancel.err').write_text(result.stderr)
        expected = checked('SET pg_yaap.enabled=off; ' + recovery_query)
        ok = (result.returncode == 0 and 'statement timeout' in result.stderr
              and result.stdout == expected)
        results.append({'case': 'pair-admission-cancel', 'pass': ok})
        if not ok:
            raise AssertionError('partition admission cancellation/reuse failed')
        query = queries['aggregate']
        # A canceled query must leave both PostgreSQL and YAAP reusable in the
        # same backend. ON_ERROR_STOP is disabled only for the expected timeout.
        sql = (setup() + '\n\\set ON_ERROR_STOP off\n'
               "SET statement_timeout='1ms'; " + query +
               " SET statement_timeout='60s';\n\\set ON_ERROR_STOP on\n" + query)
        result = run(sql)
        (OUT / 'cancel-and-reuse.out').write_text(result.stdout)
        (OUT / 'cancel-and-reuse.err').write_text(result.stderr)
        expected_after_cancel = checked('SET pg_yaap.enabled=off; ' + query)
        ok = (result.returncode == 0 and 'statement timeout' in result.stderr
              and result.stdout == expected_after_cancel)
        results.append({'case': 'cancel-and-reuse', 'pass': ok})
        if not ok:
            raise AssertionError('cancel/reuse regression failed')
    finally:
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2)+'\n')
        ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
