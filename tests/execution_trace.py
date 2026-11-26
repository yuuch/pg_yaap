#!/usr/bin/env python3
"""End-to-end Execution Trace (Layer 1) integration tests against PostgreSQL."""
import argparse
import collections
import concurrent.futures
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
PSQL = os.environ.get('PSQL', '/Users/chenyunwen/proj/postgres/installed-bench/bin/psql')
LIBRARY = Path(os.environ.get('YAAP_LIBRARY', ROOT / 'build-release/pg_yaap.dylib')).resolve()
BASE = [PSQL, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-P', 'pager=off']
OUT = Path(os.environ.get('YAAP_TEST_OUTPUT', ROOT / 'artifacts/execution-trace-tests')).resolve()
SCHEMA = f'yaap_trace_test_{os.getpid()}'


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def run(sql, timeout=120):
    return subprocess.run(BASE, input=sql, text=True, capture_output=True, timeout=timeout)


def checked(sql):
    result = run(sql)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout


def ddl(sql):
    triggers = checked("SET pg_yaap.enabled=off; SELECT evtname||':'||evtenabled::text "
                       "FROM pg_event_trigger WHERE evtname LIKE 'duckdb_%';")
    disable, restore = [], []
    for line in triggers.splitlines():
        name, state = line.rsplit(':', 1)
        qname = '"' + name.replace('"', '""') + '"'
        disable.append(f'ALTER EVENT TRIGGER {qname} DISABLE;')
        restore.append(f"ALTER EVENT TRIGGER {qname} " +
                       {'O': 'ENABLE', 'D': 'DISABLE', 'R': 'ENABLE REPLICA', 'A': 'ENABLE ALWAYS'}[state] + ';')
    checked('SET pg_yaap.enabled=off; BEGIN;' + ''.join(disable) + sql + ''.join(restore) + 'COMMIT;')


def setup(trace_dir='', workers=4, enabled=True, buffer_mb=1, application=''):
    parts = [
        f'LOAD {quote(LIBRARY)};',
        f'SET pg_yaap.enabled=on;',
        f'SET pg_yaap.parallel=on;',
        f'SET pg_yaap.parallel_max_workers={workers};',
        'SET pg_yaap.parallel_min_relation_blocks=0;',
        "SET pg_yaap.memory_limit='512MB';",
        "SET statement_timeout='90s';",
        f'SET pg_yaap.trace_enabled={"on" if enabled else "off"};',
        f'SET pg_yaap.trace_buffer_mb={buffer_mb};',
        f'SET pg_yaap.trace_output_dir={quote(trace_dir)};',
    ]
    if application:
        parts.append(f'SET application_name={quote(application)};')
    return ' '.join(parts) + ' '


def extract_trace_files(stderr):
    return [Path(m) for m in re.findall(r'pg_yaap\[trace\] file=(\S+)', stderr)]


def validate_perfetto_json(data, expect_complete=True):
    assert data.get('displayTimeUnit') == 'ms', data.get('displayTimeUnit')
    meta = data['yaap_trace']
    assert meta['format_version'] == 1
    assert meta['execution_id'] > 0
    assert meta['leader_pid'] > 0
    assert meta['worker_count'] >= 1
    assert meta['buffer_capacity_per_process'] > 0
    assert meta['query_duration_ns'] >= 0
    if expect_complete:
        assert meta['complete'] is True, meta
        assert meta['dropped_events'] == 0, meta
        assert meta['abnormal_processes'] == 0, meta

    events = data['traceEvents']
    assert isinstance(events, list) and len(events) > 0

    meta_events = [e for e in events if e.get('ph') == 'M']
    non_meta = [e for e in events if e.get('ph') != 'M']
    assert any(e['name'] == 'process_name' and 'YAAP Leader' in e['args']['name'] for e in meta_events)
    assert any(e['name'] == 'process_name' and 'YAAP Pipeline Scheduler' in e['args']['name'] for e in meta_events)

    assert len(non_meta) == meta['total_events'], (len(non_meta), meta['total_events'])

    # Validate Chrome Trace Event structure and non-negative timestamps/durations.
    worker_starts = {}
    worker_exits = {}
    pipeline_ready = {}
    pipeline_finish = {}
    tasks = []
    for ev in non_meta:
        ph = ev['ph']
        assert ph in ('X', 'i'), ev
        assert isinstance(ev['ts'], (int, float)) and ev['ts'] >= 0, ev
        assert 'cat' in ev and 'pid' in ev and 'tid' in ev, ev
        if ph == 'X':
            assert isinstance(ev['dur'], (int, float)) and ev['dur'] >= 0, ev
        else:
            assert ev.get('s') == 't', ev
        args = ev.get('args', {})
        name = ev['name']
        if name == 'Worker Start':
            worker_starts[args['worker_id']] = ev['ts']
        elif name == 'Worker Exit':
            worker_exits[args['worker_id']] = ev['ts']
        elif name.endswith(' Ready') and name.startswith('Pipeline '):
            pipeline_ready[args['pipeline_id']] = ev['ts']
        elif name.endswith(' Finish') and name.startswith('Pipeline '):
            pipeline_finish[args['pipeline_id']] = ev['ts']
        elif name.startswith('Pipeline ') and ' / Task ' in name and ph == 'X':
            tasks.append(ev)

    if expect_complete:
        assert any(e['name'] == 'Query Execute' and e['ph'] == 'X' for e in non_meta)
        assert len(tasks) > 0
        # Cross-worker timestamp consistency: every task sits within its worker's lifetime.
        for t in tasks:
            wid = t['args']['worker_id']
            assert wid in worker_starts, (wid, worker_starts)
            assert wid in worker_exits, (wid, worker_exits)
            assert worker_starts[wid] <= t['ts'] + 1.0, (worker_starts[wid], t)
            assert t['ts'] + t['dur'] <= worker_exits[wid] + 1.0, (t, worker_exits[wid])
            assert t['args']['outcome'] == 'ok', t
        # Pipeline finish must be >= pipeline ready.
        for pid, r_ts in pipeline_ready.items():
            if pid in pipeline_finish:
                assert pipeline_finish[pid] >= r_ts, (pid, r_ts, pipeline_finish[pid])

    return meta, non_meta, tasks


def run_case(name, query, trace_dir, workers=4, enabled=True, buffer_mb=1):
    expected = checked('SET pg_yaap.enabled=off; ' + query)
    case_dir = trace_dir / name
    case_dir.mkdir(parents=True, exist_ok=True)
    res = run(setup(case_dir, workers=workers, enabled=enabled, buffer_mb=buffer_mb) + query)
    (OUT / f'{name}.stderr').write_text(res.stderr)
    assert res.returncode == 0, f'{name} failed: {res.stderr}'
    if ' ORDER BY ' in query:
        assert res.stdout == expected, f'{name}: result mismatch'
    else:
        assert sorted(res.stdout.splitlines()) == sorted(expected.splitlines()), f'{name}: result mismatch'
    files = extract_trace_files(res.stderr)
    if enabled:
        assert len(files) == 1, f'{name}: expected 1 trace file, got {files}\n{res.stderr}'
        assert files[0].exists() and stat.S_IMODE(files[0].stat().st_mode) == 0o600
        data = json.loads(files[0].read_text())
        meta, non_meta, tasks = validate_perfetto_json(data, expect_complete=True)
        return files[0], data, meta, non_meta, tasks
    else:
        assert len(files) == 0
        assert 'pg_yaap[trace]' not in res.stderr
        return None, None, None, None, None


def test_security_and_export_failures(scan_query):
    # 1. Relative path rejected
    rel = run(setup('', enabled=False) + "SET pg_yaap.trace_output_dir='relative/dir';")
    assert rel.returncode != 0 and 'absolute' in rel.stderr, rel.stderr

    # 2. Path traversal '..' rejected
    trav = run(setup('', enabled=False) + "SET pg_yaap.trace_output_dir='/tmp/../etc';")
    assert trav.returncode != 0 and '..' in trav.stderr, trav.stderr

    # 3. Missing directory: SQL still succeeds with exact results, WARNING emitted
    expected = checked('SET pg_yaap.enabled=off; ' + scan_query)
    missing = run(setup(OUT / 'nonexistent_dir_xyz', workers=2, enabled=True) + scan_query)
    assert missing.returncode == 0 and missing.stdout == expected, missing.stderr
    assert 'could not create trace file' in missing.stderr, missing.stderr

    # 4. Empty trace_output_dir: SQL succeeds, NOTICE informs user
    empty_dir = run(setup('', workers=2, enabled=True) + scan_query)
    assert empty_dir.returncode == 0 and empty_dir.stdout == expected, empty_dir.stderr
    assert 'set pg_yaap.trace_output_dir' in empty_dir.stderr, empty_dir.stderr

    # 5. Unprivileged user cannot set trace_output_dir or write server files
    role = f'yaap_trace_unpriv_{os.getpid()}'
    ddl(f'CREATE ROLE {role}; GRANT USAGE ON SCHEMA {SCHEMA} TO {role}; '
        f'GRANT SELECT ON {SCHEMA}.a TO {role};')
    unpriv_dir = OUT / 'unpriv_export'
    unpriv_dir.mkdir(parents=True, exist_ok=True)
    try:
        denied = run(f'LOAD {quote(LIBRARY)}; SET ROLE {role}; '
                     f'SET pg_yaap.trace_output_dir={quote(unpriv_dir)};')
        assert denied.returncode != 0 and 'permission denied' in denied.stderr, denied.stderr

        # Pre-set by superuser, then SET ROLE unprivileged -> exporter must refuse to write file
        no_write = run(setup(unpriv_dir, workers=1, enabled=True) + f'SET ROLE {role}; ' + scan_query)
        assert no_write.returncode == 0 and no_write.stdout == expected, no_write.stderr
        assert 'requires superuser or pg_write_server_files' in no_write.stderr, no_write.stderr
        assert not list(unpriv_dir.glob('*.json'))
    finally:
        ddl(f'REVOKE SELECT ON {SCHEMA}.a FROM {role}; '
            f'REVOKE USAGE ON SCHEMA {SCHEMA} FROM {role}; DROP ROLE {role};')


def test_concurrent_sessions(scan_query):
    conc_dir = OUT / f'concurrent_{time.time_ns()}'
    conc_dir.mkdir(parents=True, exist_ok=True)
    expected = checked('SET pg_yaap.enabled=off; ' + scan_query)

    def worker_fn(idx):
        res = run(setup(conc_dir, workers=2, enabled=True) + scan_query)
        assert res.returncode == 0 and res.stdout == expected, (idx, res.stderr)
        files = extract_trace_files(res.stderr)
        assert len(files) == 1
        data = json.loads(files[0].read_text())
        validate_perfetto_json(data, expect_complete=True)
        return files[0]

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        paths = list(pool.map(worker_fn, range(4)))
    assert len(set(paths)) == 4, f'concurrent trace files collided: {paths}'


def test_abnormal_paths():
    heavy = f'SELECT sum(a.id + b.id) FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.k = b.k;'
    for kind in ('cancel', 'worker_terminate'):
        app = f'yaap_trace_{kind}_{os.getpid()}'
        ab_dir = OUT / f'{kind}_{time.time_ns()}'
        ab_dir.mkdir(parents=True, exist_ok=True)
        proc = subprocess.Popen(BASE, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)
        proc.stdin.write(setup(ab_dir, workers=4, enabled=True, application=app) + heavy)
        proc.stdin.close()
        proc.stdin = None
        target = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and proc.poll() is None:
            backend = checked(f"SELECT pid FROM pg_stat_activity "
                              f"WHERE application_name={quote(app)} AND state='active';").strip()
            if backend:
                workers = checked("SELECT pid FROM pg_stat_activity "
                                  "WHERE backend_type ILIKE '%yaap%';").split()
                if workers:
                    target = backend if kind == 'cancel' else workers[0]
                    break
            time.sleep(0.05)
        try:
            assert target, f'{kind}: query finished before injection'
            fn = 'pg_cancel_backend' if kind == 'cancel' else 'pg_terminate_backend'
            checked(f'SELECT {fn}({int(target)});')
            stdout, stderr = proc.communicate(timeout=30)
            (OUT / f'{kind}.stderr').write_text(stderr)
            assert proc.returncode != 0, stdout
            files = extract_trace_files(stderr)
            assert len(files) == 1, f'{kind}: expected partial trace file\n{stderr}'
            data = json.loads(files[0].read_text())
            meta, _, _ = validate_perfetto_json(data, expect_complete=False)
            assert meta['complete'] is False, meta
            # Verify server remains healthy and reusable afterwards.
            reuse = run(setup('', workers=2, enabled=False) + f'SELECT sum(id) FROM {SCHEMA}.a;')
            assert reuse.returncode == 0, reuse.stderr
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()


def test_overhead_comparison(query):
    perf_dir = OUT / f'perf_{time.time_ns()}'
    perf_dir.mkdir(parents=True, exist_ok=True)
    # Warm up
    run(setup('', workers=4, enabled=False) + query)
    iters = 5
    off_times, on_times = [], []
    for _ in range(iters):
        t0 = time.monotonic()
        r = run(setup('', workers=4, enabled=False) + query)
        off_times.append((time.monotonic() - t0) * 1000.0)
        assert r.returncode == 0
        t0 = time.monotonic()
        r = run(setup(perf_dir, workers=4, enabled=True) + query)
        on_times.append((time.monotonic() - t0) * 1000.0)
        assert r.returncode == 0
    off_med = sorted(off_times)[iters // 2]
    on_med = sorted(on_times)[iters // 2]
    return {'off_median_ms': round(off_med, 2), 'on_median_ms': round(on_med, 2)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--abnormal', action='store_true',
                        help='also run query cancellation and worker termination tests')
    parser.add_argument('--tpch', action='store_true',
                        help='also validate representative TPC-H queries if lineitem exists')
    args = parser.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    trace_root = OUT / f'run_{time.time_ns()}'
    trace_root.mkdir(parents=True, exist_ok=True)

    ddl(f'CREATE SCHEMA {SCHEMA}; '
        f'CREATE TABLE {SCHEMA}.a(id bigint NOT NULL, k int NOT NULL); '
        f'INSERT INTO {SCHEMA}.a SELECT i, i % 97 FROM generate_series(1, 200000) i; '
        f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {SCHEMA}.a WHERE id % 3 = 0; '
        f'ANALYZE {SCHEMA}.a; ANALYZE {SCHEMA}.b;')

    scan = f'SELECT sum(id), count(*) FROM {SCHEMA}.a;'
    dep_join = (f'SELECT a.k, sum(a.id + b.id), count(*) FROM {SCHEMA}.a a '
                f'JOIN {SCHEMA}.b b ON a.id = b.id GROUP BY a.k ORDER BY a.k;')
    fan_in = (f'SELECT l.k, l.v, r.v FROM '
              f'(SELECT k, sum(id) v FROM {SCHEMA}.a GROUP BY k) l JOIN '
              f'(SELECT k, sum(id) v FROM {SCHEMA}.a WHERE id > 10 GROUP BY k) r '
              'ON l.k = r.k ORDER BY l.k;')

    results = []
    try:
        # 1. Trace disabled
        run_case('trace_off', scan, trace_root, workers=4, enabled=False)
        results.append({'case': 'trace_off', 'pass': True})

        # 2. Single worker
        _, _, meta, _, tasks = run_case('single_worker', scan, trace_root, workers=1, enabled=True)
        assert meta['worker_count'] == 1
        assert all(t['args']['worker_id'] == 0 for t in tasks)
        results.append({'case': 'single_worker', 'pass': True, 'events': meta['total_events']})

        # 3. Multi worker
        _, _, meta, non_meta, tasks = run_case('multi_worker', scan, trace_root, workers=4, enabled=True)
        assert meta['worker_count'] == 4
        run_tasks = [t for t in tasks if t['args']['task_kind'] == 'RUN']
        assert max(collections.Counter(t['args']['pipeline_id'] for t in run_tasks).values()) == 4
        results.append({'case': 'multi_worker', 'pass': True, 'events': meta['total_events']})

        # 4. Multi-pipeline with dependencies
        sample_file, _, meta, non_meta, tasks = run_case(
            'multi_pipeline_deps', dep_join, trace_root, workers=4, enabled=True)
        assert len(meta['pipelines']) >= 3
        assert any(e['name'] == 'Worker Idle' for e in non_meta)
        assert any(e['name'].startswith('Event ') and 'Schedule' in e['name'] for e in non_meta)
        assert any(e['name'].startswith('Event ') and 'Finish' in e['name'] for e in non_meta)
        results.append({'case': 'multi_pipeline_deps', 'pass': True,
                        'pipelines': len(meta['pipelines']), 'sample_file': str(sample_file)})

        # 5. Fan-in multi-branch pipelines
        _, _, meta, _, _ = run_case('fan_in', fan_in, trace_root, workers=4, enabled=True)
        assert len(meta['pipelines']) >= 4
        results.append({'case': 'fan_in', 'pass': True, 'pipelines': len(meta['pipelines'])})

        # 6. Larger trace_buffer_mb setting (e.g. 8MB)
        _, _, meta, _, _ = run_case('buffer_8mb', scan, trace_root, workers=2, enabled=True, buffer_mb=8)
        assert meta['buffer_capacity_per_process'] >= 200000
        results.append({'case': 'buffer_8mb', 'pass': True,
                        'capacity': meta['buffer_capacity_per_process']})

        # 7. Security, permissions, and export failure resilience
        test_security_and_export_failures(scan)
        results.append({'case': 'security_and_export_failures', 'pass': True})

        # 8. Concurrent sessions
        test_concurrent_sessions(scan)
        results.append({'case': 'concurrent_sessions', 'pass': True})

        # 9. Abnormal paths (query cancel + worker termination)
        if args.abnormal:
            test_abnormal_paths()
            results.append({'case': 'abnormal_cancel_and_worker_terminate', 'pass': True})

        # 10. Overhead comparison
        perf = test_overhead_comparison(dep_join)
        results.append({'case': 'overhead_comparison', 'pass': True, **perf})

        # 11. Optional TPC-H validation
        if args.tpch:
            has_lineitem = checked(
                "SET pg_yaap.enabled=off; SELECT to_regclass('public.lineitem') IS NOT NULL;").strip()
            if has_lineitem == 't':
                tpch_queries = {
                    'tpch_q6': (
                        "SELECT sum(l_extendedprice * l_discount) AS revenue "
                        "FROM lineitem "
                        "WHERE l_shipdate >= date '1994-01-01' "
                        "  AND l_shipdate < date '1994-01-01' + interval '1' year "
                        "  AND l_discount BETWEEN 0.05 AND 0.07 "
                        "  AND l_quantity < 24;"
                    ),
                    'tpch_q1': (
                        "SELECT l_returnflag, l_linestatus, sum(l_quantity), "
                        "sum(l_extendedprice), count(*) "
                        "FROM lineitem WHERE l_shipdate <= date '1998-09-02' "
                        "GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;"
                    ),
                }
                for qname, qsql in tpch_queries.items():
                    tfile, _, tmeta, _, ttasks = run_case(qname, qsql, trace_root, workers=4, enabled=True)
                    results.append({'case': qname, 'pass': True,
                                    'events': tmeta['total_events'],
                                    'tasks': len(ttasks),
                                    'file': str(tfile)})
    finally:
        ddl(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;')
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')

    print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
