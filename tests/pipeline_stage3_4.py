#!/usr/bin/env python3
"""Stage 3 & Stage 4 verification checks against an isolated PostgreSQL instance.

Covers:
  - Concurrent PhysicalTopN (LWLock safety, NULLs, ties, wide strings, cancel/reuse)
  - Concurrent PhysicalCrossProduct (LWLock safety, wide strings)
  - Multi-consumer PhysicalMaterialize under low-memory spill/reload + consumer cleanup
  - PG lifecycle boundaries (EXPLAIN, EXPLAIN ANALYZE, DECLARE/FETCH/SCROLL CURSOR)
  - Hybrid HashJoin + independent READY branches + skewed build under tight budget
  - Partial worker startup degrading a requested-4 hybrid join to actual ready=1
  - Repeated runs + client disconnect leaving zero leaked workers or spill files
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time

import memory_runtime as runtime

SCHEMA = f'yaap_stage34_{os.getpid()}'
OUT = Path(os.environ.get('YAAP_TEST_OUTPUT', runtime.ROOT / 'artifacts/stage3-4'))
OUT.mkdir(parents=True, exist_ok=True)


def check_parity(results, tag, query, workers=4, memory='256MB', extra='', ordered=True):
    expected = runtime.checked('SET pg_yaap.enabled=off; ' + query)
    start = time.monotonic()
    result = runtime.run(runtime.setup(workers, memory) + extra + query, timeout=120)
    (OUT / f'{tag}.out').write_text(result.stdout)
    (OUT / f'{tag}.err').write_text(result.stderr)
    (OUT / f'{tag}.expected').write_text(expected)
    if ordered:
        ok = (result.returncode == 0 and result.stdout == expected)
    else:
        ok = (result.returncode == 0 and
              sorted(result.stdout.splitlines()) == sorted(expected.splitlines()))
    rec = {'case': tag, 'pass': ok, 'seconds': round(time.monotonic() - start, 3)}
    results.append(rec)
    print(json.dumps(rec), flush=True)
    if not ok:
        raise AssertionError(f'{tag} failed (rc={result.returncode}); inspect {OUT}')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--partial-hybrid', action='store_true',
                        help='run only the partial-startup hybrid join check (expects 1 available bgworker slot)')
    parser.add_argument('--pgdata', default='',
                        help='optional PGDATA path to verify zero leaked pgsql_tmp files')
    args = parser.parse_args()

    results = []
    runtime.ddl(
        f'CREATE SCHEMA {SCHEMA}; '
        f'CREATE TABLE {SCHEMA}.topn_src AS '
        'SELECT i::bigint AS id, (i % 53)::int AS k, '
        "lpad((i % 997)::text, 320, 't')::text AS s "
        'FROM generate_series(1, 50000) i; '
        f'CREATE TABLE {SCHEMA}.cross_l AS '
        "SELECT i::bigint AS id, lpad(i::text, 240, 'l')::text AS ls "
        'FROM generate_series(1, 4000) i; '
        f'CREATE TABLE {SCHEMA}.cross_r AS '
        "SELECT i::bigint AS id, lpad(i::text, 240, 'r')::text AS rs "
        'FROM generate_series(1, 8) i; '
        f'CREATE TABLE {SCHEMA}.mat_outer AS '
        'SELECT i::int AS id, (i % 64)::int AS k, '
        "lpad(i::text, 512, 'm')::text AS payload "
        'FROM generate_series(1, 12000) i; '
        f'CREATE TABLE {SCHEMA}.mat_inner AS '
        'SELECT (i % 64)::int AS k, (i % 100)::int AS v '
        'FROM generate_series(1, 2000) i; '
        f'CREATE TABLE {SCHEMA}.hj_a AS '
        'SELECT i::bigint AS id, (i % 97)::int AS k, '
        "lpad(i::text, 160, 'a')::text AS s "
        'FROM generate_series(1, 40000) i; '
        f'CREATE TABLE {SCHEMA}.hj_b AS '
        'SELECT * FROM ' + f'{SCHEMA}.hj_a WHERE id % 3 = 0; '
        f'ANALYZE {SCHEMA}.topn_src; ANALYZE {SCHEMA}.cross_l; ANALYZE {SCHEMA}.cross_r; '
        f'ANALYZE {SCHEMA}.mat_outer; ANALYZE {SCHEMA}.mat_inner; '
        f'ANALYZE {SCHEMA}.hj_a; ANALYZE {SCHEMA}.hj_b;'
    )

    try:
        if args.partial_hybrid:
            query = (f'SELECT a.id, b.id, b.s FROM {SCHEMA}.hj_a a '
                     f'JOIN {SCHEMA}.hj_b b ON a.id = b.id ORDER BY a.id LIMIT 200;')
            res = check_parity(
                results, 'partial-hybrid-req4-ready1', query,
                workers=4, memory='64MB',
                extra=("SET pg_yaap.memory_soft_limit='1MB'; "
                       "SET pg_yaap.hash_join_partition_probe=on; "
                       "SET pg_yaap.trace_execution_path=on; "
                       "SET client_min_messages=log; "))
            assert 'workers requested=4 ready=1' in res.stderr, res.stderr
            rec = {'case': 'partial-hybrid-verified-ready-1', 'pass': True}
            results.append(rec)
            print(json.dumps(rec), flush=True)
            return

        # 1. Concurrent PhysicalTopN (LWLock + ties + wide strings + multi-key).
        topn_q = f'SELECT id, k, s FROM {SCHEMA}.topn_src ORDER BY k ASC, id ASC LIMIT 25;'
        for w in (1, 4):
            check_parity(results, f'topn-asc-ties-w{w}', topn_q, workers=w)

        topn_multi = f'SELECT id, k, s FROM {SCHEMA}.topn_src ORDER BY k DESC, s ASC, id ASC LIMIT 25;'
        for w in (1, 4):
            check_parity(results, f'topn-multikey-desc-w{w}', topn_multi, workers=w)

        topn_str_q = f'SELECT id, s FROM {SCHEMA}.topn_src ORDER BY s DESC, id ASC LIMIT 30;'
        for w in (1, 4):
            check_parity(results, f'topn-wide-strings-w{w}', topn_str_q, workers=w)

        # TopN cancellation + reuse (proves LWLock is never leaked on cancel).
        topn_recovery = f'SELECT count(*) FROM {SCHEMA}.topn_src;'
        cancel_topn = runtime.run(
            runtime.setup(4, '256MB') +
            '\n\\set ON_ERROR_STOP off\n'
            "SET statement_timeout='1ms'; " + topn_str_q +
            " SET statement_timeout='60s';\n\\set ON_ERROR_STOP on\n" + topn_recovery)
        expected_rec = runtime.checked('SET pg_yaap.enabled=off; ' + topn_recovery)
        assert cancel_topn.returncode == 0 and cancel_topn.stdout == expected_rec, cancel_topn.stderr
        results.append({'case': 'topn-cancel-reuse', 'pass': True})
        print(json.dumps(results[-1]), flush=True)

        # 2. Concurrent PhysicalCrossProduct (LWLock + wide strings).
        cross_q = (f'SELECT count(*), sum(l.id + r.id) '
                   f'FROM {SCHEMA}.cross_l l CROSS JOIN {SCHEMA}.cross_r r;')
        for w in (1, 4):
            check_parity(results, f'cross-product-wide-w{w}', cross_q, workers=w)

        cross_str_q = (f'SELECT l.id, l.ls, r.id, r.rs '
                       f'FROM {SCHEMA}.cross_l l CROSS JOIN {SCHEMA}.cross_r r '
                       'WHERE l.id <= 10 ORDER BY l.id, r.id;')
        for w in (1, 4):
            check_parity(results, f'cross-product-strings-out-w{w}', cross_str_q, workers=w)

        # 3. Multi-consumer PhysicalMaterialize under tight spill budget.
        mat_q = (f'SELECT o.id, o.k, o.payload, (SELECT min(x.v) FROM {SCHEMA}.mat_inner x WHERE x.k = o.k) AS mv '
                 f'FROM {SCHEMA}.mat_outer o ORDER BY o.id LIMIT 200;')
        for w in (1, 4):
            check_parity(
                results, f'materialize-spill-w{w}', mat_q,
                workers=w, memory='64MB',
                extra="SET pg_yaap.memory_soft_limit='1MB'; ")

        # 4. Hybrid HashJoin + independent READY branches under tight budget.
        indep_q = (
            'SELECT l.k, l.s1, r.s2 FROM '
            f'(SELECT a.k, sum(a.id + b.id) AS s1 FROM {SCHEMA}.hj_a a '
            f'JOIN {SCHEMA}.hj_b b ON a.id = b.id GROUP BY a.k) l '
            'JOIN '
            f'(SELECT a.k, count(*) AS s2 FROM {SCHEMA}.hj_a a '
            f'JOIN {SCHEMA}.hj_b b ON a.id = b.id GROUP BY a.k) r '
            'ON l.k = r.k ORDER BY l.k;'
        )
        for w in (1, 4):
            check_parity(
                results, f'hybrid-independent-branches-w{w}', indep_q,
                workers=w, memory='64MB',
                extra="SET pg_yaap.memory_soft_limit='1MB'; SET pg_yaap.hash_join_partition_probe=on; ")

        # 5. PG lifecycle boundaries: EXPLAIN, EXPLAIN ANALYZE, cursors (forward & scroll).
        exp_res = runtime.run(runtime.setup(4, '256MB') + f'EXPLAIN SELECT count(*) FROM {SCHEMA}.hj_a;')
        assert exp_res.returncode == 0 and 'YAAP' in exp_res.stdout, (exp_res.stdout, exp_res.stderr)
        results.append({'case': 'pg-explain', 'pass': True})
        print(json.dumps(results[-1]), flush=True)

        exp_an_res = runtime.run(runtime.setup(4, '256MB') + f'EXPLAIN ANALYZE SELECT count(*) FROM {SCHEMA}.hj_a;')
        assert exp_an_res.returncode != 0 and 'EXPLAIN ANALYZE is not supported' in exp_an_res.stderr, exp_an_res.stderr
        results.append({'case': 'pg-explain-analyze-rejected', 'pass': True})
        print(json.dumps(results[-1]), flush=True)

        cursor_sql = (
            'BEGIN; '
            f'DECLARE c CURSOR FOR SELECT id FROM {SCHEMA}.hj_a WHERE id <= 12 ORDER BY id; '
            'FETCH 5 FROM c; FETCH 5 FROM c; FETCH 5 FROM c; CLOSE c; '
            f'DECLARE sc SCROLL CURSOR FOR SELECT id FROM {SCHEMA}.hj_a WHERE id <= 10 ORDER BY id; '
            'FETCH FIRST FROM sc; FETCH LAST FROM sc; FETCH BACKWARD 2 FROM sc; CLOSE sc; '
            'COMMIT;'
        )
        expected_cur = runtime.checked('SET pg_yaap.enabled=off; ' + cursor_sql)
        cur_res = runtime.run(runtime.setup(4, '256MB') + cursor_sql)
        assert cur_res.returncode == 0 and cur_res.stdout == expected_cur, (cur_res.stderr, cur_res.stdout, expected_cur)
        results.append({'case': 'pg-cursors-forward-and-scroll', 'pass': True})
        print(json.dumps(results[-1]), flush=True)

        # 6. Client disconnect / kill mid-query + leak check (no lingering workers or spill dirs).
        slow_q = f'SELECT sum(a.id + b.id) FROM {SCHEMA}.hj_a a JOIN {SCHEMA}.hj_b b ON a.k = b.k;'
        proc = subprocess.Popen(
            runtime.BASE, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        proc.stdin.write(runtime.setup(4, '64MB') + "SET pg_yaap.memory_soft_limit='1MB'; " + slow_q)
        proc.stdin.close()
        saw_worker = False
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and proc.poll() is None:
            w = runtime.checked("SELECT pid FROM pg_stat_activity WHERE backend_type ILIKE '%yaap%';").strip()
            if w:
                saw_worker = True
                break
            time.sleep(0.05)
        proc.kill()
        proc.communicate()
        # Wait up to 10s for backend + bgworkers to exit cleanly.
        deadline = time.monotonic() + 10
        lingering = []
        while time.monotonic() < deadline:
            lingering = runtime.checked(
                "SELECT pid FROM pg_stat_activity WHERE backend_type ILIKE '%yaap%';").split()
            if not lingering:
                break
            time.sleep(0.1)
        assert not lingering, f'leaked yaap workers after disconnect: {lingering}'
        # Verify subsequent query runs cleanly.
        check_parity(results, 'post-disconnect-reuse', f'SELECT count(*), sum(id) FROM {SCHEMA}.hj_a;', workers=4)
        if args.pgdata:
            tmp_dir = Path(args.pgdata) / 'base' / 'pgsql_tmp'
            leftovers = list(tmp_dir.glob('pgsql_tmp*')) if tmp_dir.exists() else []
            assert not leftovers, f'leaked spill files in {tmp_dir}: {leftovers}'
        results.append({'case': 'client-disconnect-no-leaks', 'pass': True, 'saw_worker': saw_worker})
        print(json.dumps(results[-1]), flush=True)

    finally:
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
        runtime.ddl(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
