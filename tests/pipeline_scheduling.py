#!/usr/bin/env python3
"""Assert complete logical tasks, independent of their executing processes."""
import argparse
import json
import os
import time

import pipeline_timeline as timeline

SCHEMA = f'yaap_schedule_{os.getpid()}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--queue-pressure', action='store_true',
                        help='also run a 65-worker cohort against the 64-entry ready queue')
    args = parser.parse_args()
    worker_counts = (1, 4, 65) if args.queue_pressure else (1, 4)
    if args.queue_pressure:
        assert int(timeline.checked('SHOW max_worker_processes;')) >= 65
    timeline.OUT.mkdir(parents=True, exist_ok=True)
    timeline.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {SCHEMA}.a AS '
                 'SELECT i::bigint id,(i%97)::int k FROM generate_series(1,500000) i; '
                 f'ANALYZE {SCHEMA}.a;')
    query = f'SELECT sum(id),count(*) FROM {SCHEMA}.a;'
    try:
        expected = timeline.checked('SET pg_yaap.enabled=off;' + query)
        for workers in worker_counts:
            prefix = timeline.OUT / f'scheduling-w{workers}-{time.time_ns()}'
            config = timeline.setup(prefix, workers=workers)
            result = timeline.run(config + query)
            assert result.returncode == 0, result.stderr
            assert result.stdout == expected, (workers, result.stdout, expected)
            files = list(timeline.OUT.glob(prefix.name + '.*.json'))
            assert len(files) == 1, files
            data = json.loads(files[0].read_text())
            timeline.validate(data)
            pipelines = data['yaap']['pipelines']
            runs = [e for e in data['traceEvents'] if e['ph'] == 'X' and e['name'].endswith(' / run')]
            by_pipeline = {}
            for event in runs:
                by_pipeline.setdefault(event['args']['pipeline_id'], []).append(event)
            cohort = max(by_pipeline.values(), key=len)
            assert len(cohort) == workers, cohort
            assert {int(e['args']['task_id']) & 0xffffffff for e in cohort} == set(range(workers))
            assert all(e['args']['result'] == 'finished' for e in runs)
            leader_tasks = [w for p in pipelines for w in p['workers']
                            if w['worker_id'] == -1 and
                            (w['task_count'] or w['finalize_task_count'])]
            assert not leader_tasks, pipelines
            assert all(sum(w['finalize_task_count'] for w in p['workers']) == 1
                       for p in pipelines), pipelines
            # Every scheduled RUN must finish, including empty scan shares.
            assert all(p['finished_ns'] > 0 for p in pipelines)
            print(f'workers={workers} complete_tasks={workers}: PASS', flush=True)
    finally:
        timeline.ddl(f'DROP SCHEMA IF EXISTS {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
