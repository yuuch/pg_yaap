#!/usr/bin/env python3
"""Assert complete logical tasks, independent of their executing processes."""
import argparse
import json
import os
import time

import execution_trace as trace

SCHEMA = f'yaap_schedule_{os.getpid()}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue-pressure', action='store_true',
                        help='also run a 65-worker cohort against the 64-entry ready queue')
    args = parser.parse_args()
    worker_counts = (1, 4, 65) if args.queue_pressure else (1, 4)
    if args.queue_pressure:
        assert int(trace.checked('SHOW max_worker_processes;')) >= 65
    trace.OUT.mkdir(parents=True, exist_ok=True)
    trace.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {SCHEMA}.a AS '
              'SELECT i::bigint id,(i%97)::int k FROM generate_series(1,500000) i; '
              f'ANALYZE {SCHEMA}.a;')
    query = f'SELECT sum(id),count(*) FROM {SCHEMA}.a;'
    try:
        expected = trace.checked('SET pg_yaap.enabled=off;' + query)
        for workers in worker_counts:
            case_dir = trace.OUT / f'scheduling-w{workers}-{time.time_ns()}'
            case_dir.mkdir(parents=True, exist_ok=True)
            config = trace.setup(case_dir, workers=workers, enabled=True)
            result = trace.run(config + query)
            assert result.returncode == 0, result.stderr
            assert result.stdout == expected, (workers, result.stdout, expected)
            files = trace.extract_trace_files(result.stderr)
            assert len(files) == 1, files
            data = json.loads(files[0].read_text())
            meta, non_meta, tasks = trace.validate_perfetto_json(data, expect_complete=True)
            runs = [e for e in tasks if e['args']['task_kind'] == 'RUN']
            by_pipeline = {}
            for event in runs:
                by_pipeline.setdefault(event['args']['pipeline_id'], []).append(event)
            cohort = max(by_pipeline.values(), key=len)
            assert len(cohort) == workers, cohort
            assert {int(e['args']['task_id']) for e in cohort} == set(range(workers))
            assert all(e['args']['outcome'] == 'ok' for e in runs)
            assert not any(e['args']['worker_id'] == -1 for e in tasks), tasks
            finishes = [e for e in non_meta if e['name'].startswith('Pipeline ') and e['name'].endswith(' Finish')]
            assert len(finishes) == len(meta['pipelines'])
            print(f'workers={workers} complete_tasks={workers}: PASS', flush=True)
    finally:
        trace.ddl(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
