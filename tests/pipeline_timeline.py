#!/usr/bin/env python3
"""Pipeline timeline integration checks against a dedicated PostgreSQL instance."""
import argparse
import collections
import json
import os
from pathlib import Path
import subprocess
import stat
import time

ROOT = Path(__file__).resolve().parents[1]
PSQL = os.environ.get('PSQL', '/Users/chenyunwen/proj/postgres/installed-bench/bin/psql')
LIBRARY = Path(os.environ.get('YAAP_LIBRARY', ROOT / 'build-bench/pg_yaap.dylib')).resolve()
BASE = [PSQL, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1', '-P', 'pager=off']
OUT = Path(os.environ.get('YAAP_TEST_OUTPUT', ROOT / 'artifacts/pipeline-timeline-tests')).resolve()
SCHEMA = f'yaap_timeline_{os.getpid()}'


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
    triggers = checked("SET pg_yaap.enabled=off; SELECT evtname||':'||evtenabled::text FROM pg_event_trigger WHERE evtname LIKE 'duckdb_%';")
    disable, restore = [], []
    for line in triggers.splitlines():
        name, state = line.rsplit(':', 1)
        name = '"' + name.replace('"', '""') + '"'
        disable.append(f'ALTER EVENT TRIGGER {name} DISABLE;')
        restore.append(f"ALTER EVENT TRIGGER {name} " + {'O': 'ENABLE', 'D': 'DISABLE', 'R': 'ENABLE REPLICA', 'A': 'ENABLE ALWAYS'}[state] + ';')
    checked('SET pg_yaap.enabled=off; BEGIN;' + ''.join(disable) + sql + ''.join(restore) + 'COMMIT;')


def setup(prefix, workers=4, mode='trace', capacity=8192, legacy=False, application='', scan_mode='off', io_mode='off'):
    return (f'LOAD {quote(LIBRARY)}; SET application_name={quote(application)}; '
            f'SET pg_yaap.parallel_max_workers={workers}; SET pg_yaap.parallel_min_relation_blocks=0; '
            "SET pg_yaap.memory_limit='512MB'; SET statement_timeout='90s'; "
            f'SET pg_yaap.profile={"on" if legacy else "off"}; '
            f'SET pg_yaap.profile_timeline={mode}; SET pg_yaap.profile_trace_capacity={capacity}; '
            f'SET pg_yaap.profile_trace_file={quote(prefix)}; SET pg_yaap.profile_scan={scan_mode}; SET pg_yaap.profile_scan_io={io_mode}; ')


def validate(data, dropped_ok=False):
    meta = data['yaap']
    assert meta['schema_version'] == 1
    assert meta['query_origin_ns'] > 0 and meta['query_end_ns'] >= 0
    if not dropped_ok:
        assert meta['complete'] and not meta['dropped_events'] and not meta['incomplete_tasks']
    sums = collections.Counter()
    repeated = collections.Counter()
    timestamps = [e['ts'] for e in data['traceEvents'] if e['ph'] != 'M']
    assert timestamps == sorted(timestamps)
    for event in data['traceEvents']:
        if event['ph'] == 'M':
            continue
        assert event['ts'] >= 0
        if event['ph'] != 'X':
            continue
        assert event['dur'] >= 0
        args = event['args']
        assert args['os_pid'] > 0
        if event['name'].endswith(' / run') or event['name'].endswith(' / combine'):
            sums[(args['pipeline_id'], event['tid'])] += event['dur'] * 1000
            repeated[(args['pipeline_id'], event['tid'], args['task_id'])] += 1
    if meta['complete']:
        expected_fragments = sum(p['task_count'] + sum(w['finalize_task_count'] for w in p['workers'])
                                 for p in meta['pipelines'])
        assert sum(e['ph'] == 'X' for e in data['traceEvents']) + meta['dropped_events'] == expected_fragments
    by_id = {p['pipeline_id']: p for p in meta['pipelines']}
    for pipeline in meta['pipelines']:
        if meta['complete']:
            for parent in pipeline['dependencies']:
                assert by_id[parent]['finished_ns'] <= pipeline['ready_ns']
        workers = pipeline['workers']
        active = [w for w in workers if w['task_count']]
        assert pipeline['worker_count'] == len(active)
        assert pipeline['task_count'] == sum(w['task_count'] for w in workers)
        assert pipeline['total_worker_active_ns'] == sum(w['active_ns'] for w in workers)
        if active:
            assert pipeline['first_start_ns'] == min(w['first_start_ns'] for w in active)
            assert pipeline['last_end_ns'] == max(w['last_end_ns'] for w in active)
            assert pipeline['wall_span_ns'] == pipeline['last_end_ns'] - pipeline['first_start_ns']
        finalizers = [w for w in workers if w['finalize_task_count']]
        if finalizers:
            assert pipeline['finalize_start_ns'] == min(w['finalize_start_ns'] for w in finalizers)
            assert pipeline['finalize_end_ns'] == max(w['finalize_end_ns'] for w in finalizers)
            assert pipeline['finalize_start_ns'] >= pipeline['last_end_ns']
        for w in workers:
            assert w['pid'] > 0
            assert w['active_ns'] == w['run_active_ns'] + w['combine_active_ns']
            assert w['last_end_ns'] >= w['first_start_ns']
            assert w['active_ns'] <= w['last_end_ns'] - w['first_start_ns']
            assert w['finalize_end_ns'] >= w['finalize_start_ns']
            if not meta['dropped_events']:
                # Export rounds to microseconds; allow <1us per execution slice.
                assert abs(sums[(pipeline['pipeline_id'], w['slot_id'])] - w['active_ns']) <= (w['task_count'] + 1) * 1000
    return repeated


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--abnormal', action='store_true', help='also cancel a query and terminate one YAAP worker')
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {SCHEMA}.a(id bigint NOT NULL,k int NOT NULL); '
        f'INSERT INTO {SCHEMA}.a SELECT i,i%97 FROM generate_series(1,200000) i; '
        f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {SCHEMA}.a WHERE id%3=0; ANALYZE {SCHEMA}.a; ANALYZE {SCHEMA}.b;')
    scan = f'SELECT sum(id),count(*) FROM {SCHEMA}.a;'
    complex_sql = (f'SELECT a.k,sum(a.id+b.id),count(*) FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.id=b.id '
                   'GROUP BY a.k ORDER BY a.k;')
    try:
        for name, query, workers, mode, capacity, legacy in [
            ('single_pipeline', f'SELECT id FROM {SCHEMA}.a WHERE id<=10;', 1, 'trace', 8192, False),
            ('serial_output_cap', f'SELECT id FROM {SCHEMA}.a WHERE id<=10;', 4, 'trace', 8192, False),
            ('single', scan, 1, 'trace', 8192, False),
            ('multi', scan, 4, 'trace', 8192, False),
            ('dependencies', complex_sql, 4, 'trace', 8192, False),
            ('parallel_finalize', complex_sql, 4, 'trace', 8192, False),
            ('empty', f'SELECT id FROM {SCHEMA}.a WHERE id<0;', 4, 'trace', 8192, False),
            ('overflow', complex_sql, 4, 'trace', 1, False),
            ('basic', scan, 4, 'basic', 8192, False),
            ('off', scan, 4, 'off', 8192, False),
            ('legacy', scan, 4, 'off', 8192, True),
            ('both_profilers', scan, 4, 'trace', 8192, True),
            ('join_profile_stages', complex_sql, 4, 'trace', 8192, True),
        ]:
            prefix = OUT / f'{name}-{time.time_ns()}'
            expected = checked('SET pg_yaap.enabled=off;' + query)
            config = setup(prefix, workers, mode, capacity, legacy)
            if name == 'parallel_finalize':
                config += 'SET pg_yaap.hash_join_partition_probe=off;'
            result = run(config + query)
            (OUT / f'{name}.stderr').write_text(result.stderr)
            assert result.returncode == 0, result.stderr
            assert sorted(result.stdout.splitlines()) == sorted(expected.splitlines()), (name, result.stdout, expected)
            files = list(OUT.glob(prefix.name + '.*.json'))
            if mode == 'trace':
                assert len(files) == 1, (name, files)
                assert stat.S_IMODE(files[0].stat().st_mode) == 0o600
                data = json.loads(files[0].read_text())
                repeats = validate(data, name == 'overflow')
                if name in ('single_pipeline', 'serial_output_cap'):
                    assert len(data['yaap']['pipelines']) == 1
                    assert data['yaap']['pipelines'][0]['worker_count'] == 1
                if name == 'multi':
                    assert any(p['worker_count'] == workers for p in data['yaap']['pipelines'])
                if name == 'single':
                    assert max(repeats.values(), default=0) > 1, 'expected repeated Execute slices for one task'
                if name == 'dependencies':
                    pipelines = data['yaap']['pipelines']
                    assert any(p['dependencies'] for p in pipelines)
                    assert any(p['finalize_end_ns'] > p['finalize_start_ns'] for p in pipelines)
                    counts = collections.Counter(w['slot_id'] for p in pipelines for w in p['workers'] if w['task_count'])
                    assert max(counts.values()) > 1
                if name == 'overflow':
                    assert data['yaap']['dropped_events'] > 0
                if name == 'parallel_finalize':
                    assert any(w['worker_id'] >= 0 and w['finalize_task_count']
                               for p in data['yaap']['pipelines'] for w in p['workers'])
            else:
                assert not files
            if mode == 'basic':
                assert 'pg_yaap[timeline]' in result.stderr
                assert 'pg_yaap[timing]' not in result.stderr
            if mode == 'off' and not legacy:
                assert 'timeline' not in result.stderr.lower()
            if legacy:
                assert 'pg_yaap[timing]' in result.stderr
            if name == 'join_profile_stages':
                for stage in ('hash_join_build_append', 'hash_join_hybrid_execute',
                              'hash_join_hybrid_direct_rows'):
                    assert f'stage={stage} ' in result.stderr, stage
            results.append({'case': name, 'pass': True, 'files': [str(p) for p in files]})
        if args.abnormal:
            abnormal_checks(results, complex_sql)
            prefix = OUT / f'startup-failure-{time.time_ns()}'
            failure = run(setup(prefix, workers=64) + scan)
            (OUT / 'startup-failure.stderr').write_text(failure.stderr)
            assert failure.returncode != 0
            files = list(OUT.glob(prefix.name + '.*.json'))
            assert len(files) == 1
            data = json.loads(files[0].read_text())
            assert not data['yaap']['complete']
            assert all(p['task_count'] == 0 for p in data['yaap']['pipelines'])
            results.append({'case': 'startup-failure-no-tasks', 'pass': True, 'partial_files': [str(p) for p in files]})
        export_checks(results, scan)
    finally:
        ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))


def export_checks(results, scan):
    prefix = OUT / f'permissions-{time.time_ns()}'
    relative = run(setup(prefix, mode='off') + "SET pg_yaap.profile_trace_file='relative.json';")
    assert relative.returncode != 0 and 'absolute' in relative.stderr
    expected = checked('SET pg_yaap.enabled=off;' + scan)
    missing = run(setup(OUT / 'absent-directory' / 'trace') + scan)
    assert missing.returncode == 0 and missing.stdout == expected
    assert 'could not create timeline file' in missing.stderr
    role = f'yaap_timeline_export_{os.getpid()}'
    ddl(f'CREATE ROLE {role}; GRANT USAGE ON SCHEMA {SCHEMA} TO {role}; GRANT SELECT ON {SCHEMA}.a TO {role};')
    try:
        denied = run(f'LOAD {quote(LIBRARY)}; SET ROLE {role}; SET pg_yaap.profile_trace_file={quote(prefix)};')
        assert denied.returncode != 0 and 'permission denied' in denied.stderr
        # An administrator may have configured a prefix before SET ROLE. The
        # exporter must still check the executing role, not just GUC access.
        no_export = run(setup(prefix, workers=1) + f'SET ROLE {role};' + scan)
        assert no_export.returncode == 0 and no_export.stdout == expected, no_export.stderr
        assert 'export requires superuser or pg_write_server_files' in no_export.stderr
        assert not list(OUT.glob(prefix.name + '.*.json'))
    finally:
        ddl(f'REVOKE SELECT ON {SCHEMA}.a FROM {role}; REVOKE USAGE ON SCHEMA {SCHEMA} FROM {role}; DROP ROLE {role};')
    results.append({'case': 'export-permissions-and-failure', 'pass': True})


def abnormal_checks(results, query, scan_mode='off', io_mode='off'):
    # Repeat the real join until a worker is visible; do not depend on SQL sleeps.
    heavy = f'SELECT sum(a.id+b.id) FROM {SCHEMA}.a a JOIN {SCHEMA}.b b ON a.k=b.k;'
    for kind in ('cancel', 'worker_error'):
        app = f'yaap_timeline_{kind}_{os.getpid()}'
        prefix = OUT / f'{kind}-{time.time_ns()}'
        process = subprocess.Popen(BASE, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.stdin.write(setup(prefix, application=app, scan_mode=scan_mode, io_mode=io_mode) + heavy)
        process.stdin.close()
        process.stdin = None
        target = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and process.poll() is None:
            backend = checked(f"SELECT pid FROM pg_stat_activity WHERE application_name={quote(app)} AND state='active';").strip()
            if backend:
                # YAAP dynamic workers are not PostgreSQL parallel workers and may
                # have no leader_pid; identify the runtime's explicit worker type.
                workers = checked("SELECT pid FROM pg_stat_activity WHERE backend_type ILIKE '%yaap%';").split()
                if workers:
                    target = backend if kind == 'cancel' else workers[0]
                    break
            time.sleep(0.05)
        try:
            assert target, 'query finished or no YAAP worker observed before injection'
            function = 'pg_cancel_backend' if kind == 'cancel' else 'pg_terminate_backend'
            checked(f'SELECT {function}({int(target)});')
            stdout, stderr = process.communicate(timeout=30)
            (OUT / f'{kind}.stderr').write_text(stderr)
            assert process.returncode != 0, stdout
            if io_mode != 'off':
                assert 'pg_yaap[scan-io]' in stderr and 'complete=no' in stderr
            if scan_mode != 'off':
                assert 'pg_yaap[scan]' in stderr and 'complete=no' in stderr
            files = list(OUT.glob(prefix.name + '.*.json'))
            assert files, 'failed query must publish a partial timeline'
            for file in files:
                data = json.loads(file.read_text())
                validate(data, True)
                assert not data['yaap']['complete']
                assert any(p['task_count'] == 0 for p in data['yaap']['pipelines'])
            # A new backend can still run YAAP after failed-query cleanup.
            reuse = run(setup(OUT / f'{kind}-reuse', mode='off') + f'SELECT sum(id) FROM {SCHEMA}.a;')
            assert reuse.returncode == 0, reuse.stderr
            results.append({'case': kind, 'pass': True, 'partial_files': [str(p) for p in files]})
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == '__main__':
    main()
