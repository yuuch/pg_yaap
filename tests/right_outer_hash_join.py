#!/usr/bin/env python3
"""Right/full-preservation semantics for YAAP's adaptive hash join.

Requires the dedicated PostgreSQL test instance used by memory_runtime.py.
Every query is compared byte-for-byte with native PostgreSQL. Fixtures and
profile artifacts are isolated under a process-specific schema/output folder.
"""
import json
import os
from pathlib import Path
import time

import memory_runtime as runtime

SCHEMA = f'yaap_right_join_{os.getpid()}'
OUT = Path(os.environ.get('YAAP_TEST_OUTPUT', runtime.ROOT / 'artifacts/right-outer-hash-join'))
OUT.mkdir(parents=True, exist_ok=True)


def main():
    results = []
    a = f'{SCHEMA}.small_preserved'
    b = f'{SCHEMA}.large_probe'
    bs = f'{SCHEMA}.small_probe'
    empty = f'{SCHEMA}.empty'
    aw = f'{SCHEMA}.wide_preserved'
    bw = f'{SCHEMA}.wide_probe'
    runtime.ddl(
        f'CREATE SCHEMA {SCHEMA}; '
        f'CREATE TABLE {a}(id int, k int, score_limit int, label text); '
        f"INSERT INTO {a} VALUES (1,1,5,'c1'),(2,1,100,'c2'),(3,2,NULL,'c3'),(4,NULL,99,'c4'),"
        "(5,3,10,'c5'),(6,4,0,'c6'),(7,5,1000,'c7'),(8,6,10,'c8'); "
        f'CREATE TABLE {b} AS SELECT i::int AS id, ((i-1)%5+1)::int AS k, '
        '(i%20)::int AS score FROM generate_series(1,60000) i; '
        f'CREATE TABLE {bs} AS SELECT * FROM {b} WHERE id<20; '
        f'CREATE TABLE {empty} AS SELECT * FROM {a} WHERE false; '
        f'CREATE TABLE {aw} AS SELECT i::int AS id, (i%97)::int AS k, '
        "lpad(i::text,512,'x')::text AS payload FROM generate_series(1,8000) i; "
        f'CREATE TABLE {bw} AS SELECT i::int AS id, ((i-1)%5+1)::int AS k '
        'FROM generate_series(1,64000) i; '
        f'ANALYZE {a}; ANALYZE {b}; ANALYZE {bs}; ANALYZE {empty}; ANALYZE {aw}; ANALYZE {bw};')

    cases = [
        ('left-swapped-small-build',
         f'SELECT a.id,count(b.id) FROM {a} a LEFT JOIN {b} b '
         'ON a.k=b.k AND b.score<a.score_limit GROUP BY a.id ORDER BY a.id;',
         4, '256MB', True),
        ('right-preserved-small-build',
         f'SELECT a.id,count(b.id) FROM {b} b RIGHT JOIN {a} a '
         'ON a.k=b.k AND b.score<a.score_limit GROUP BY a.id ORDER BY a.id;',
         4, '256MB', True),
        ('left-swapped-small-build-w1',
         f'SELECT a.id,count(b.id) FROM {a} a LEFT JOIN {b} b '
         'ON a.k=b.k AND b.score<a.score_limit GROUP BY a.id ORDER BY a.id;',
         1, '256MB', True),
        ('left-swapped-small-build-w12',
         f'SELECT a.id,count(b.id) FROM {a} a LEFT JOIN {b} b '
         'ON a.k=b.k AND b.score<a.score_limit GROUP BY a.id ORDER BY a.id;',
         12, '256MB', True),
        ('right-preserved-small-build-w12',
         f'SELECT a.id,count(b.id) FROM {b} b RIGHT JOIN {a} a '
         'ON a.k=b.k AND b.score<a.score_limit GROUP BY a.id ORDER BY a.id;',
         12, '256MB', True),
        ('left-no-valid-match',
         f'SELECT a.id,count(b.id) FROM {a} a LEFT JOIN {b} b '
         'ON a.k=b.k AND b.score < 0 GROUP BY a.id ORDER BY a.id;',
         4, '256MB', True),
        ('right-no-valid-match',
         f'SELECT a.id,count(b.id) FROM {b} b RIGHT JOIN {a} a '
         'ON a.k=b.k AND b.score < 0 GROUP BY a.id ORDER BY a.id;',
         4, '256MB', True),
        ('left-swapped-string-null-extension',
         f'SELECT a.id,a.label,b.id FROM {a} a LEFT JOIN {bs} b '
         'ON a.k=b.k AND b.score<a.score_limit;',
         4, '256MB', False),
        ('left-swapped-string-null-extension-ordered',
         f'SELECT a.id,a.label,b.id FROM {a} a LEFT JOIN {bs} b '
         'ON a.k=b.k AND b.score<a.score_limit ORDER BY a.id,b.id;',
         4, '256MB', False),
        ('left-empty-probe',
         f'SELECT a.id,count(b.id) FROM {a} a LEFT JOIN {empty} b ON a.id=b.id '
         'GROUP BY a.id ORDER BY a.id;',
         4, '256MB', False),
        ('right-empty-preserved',
         f'SELECT a.id,count(b.id) FROM {b} b RIGHT JOIN {empty} a ON a.id=b.id '
         'GROUP BY a.id ORDER BY a.id;',
         4, '256MB', False),
        ('left-empty-preserved',
         f'SELECT a.id,count(b.id) FROM {empty} a LEFT JOIN {b} b ON a.id=b.id '
         'GROUP BY a.id ORDER BY a.id;',
         4, '256MB', False),
        ('left-spilled-build-segments',
         f'SELECT a.id,count(b.id) FROM {aw} a LEFT JOIN {bw} b '
         "ON a.k=b.k AND a.payload LIKE 'x%' GROUP BY a.id ORDER BY a.id;",
         4, '64MB', True),
    ]
    try:
        for name, query, workers, memory, _ in cases:
            expected = runtime.checked('SET pg_yaap.enabled=off; ' + query)
            extra_sql = ''
            if name == 'left-spilled-build-segments':
                extra_sql = "SET pg_yaap.memory_soft_limit='1MB'; "
            start = time.monotonic()
            result = runtime.run(runtime.setup(workers, memory) + extra_sql + query, timeout=120)
            (OUT / f'{name}.out').write_text(result.stdout)
            (OUT / f'{name}.err').write_text(result.stderr)
            (OUT / f'{name}.expected').write_text(expected)
            output_matches = result.stdout == expected
            if name == 'left-swapped-string-null-extension':
                output_matches = sorted(result.stdout.splitlines()) == sorted(expected.splitlines())
            ok = result.returncode == 0 and output_matches
            row = {'case': name, 'pass': ok, 'seconds': round(time.monotonic()-start, 3)}
            results.append(row)
            print(json.dumps(row), flush=True)
            if not ok:
                raise AssertionError(f'{name}: result mismatch; inspect {OUT}')

        recovery = f'SELECT count(*) FROM {aw};'
        expected_recovery = runtime.checked('SET pg_yaap.enabled=off; ' + recovery)
        slow = f'SELECT sum(a.id+b.id) FROM {aw} a LEFT JOIN {bw} b '
        slow += "ON a.k=b.k AND a.payload LIKE 'x%';"
        cancel_sql = (runtime.setup(4, '64MB') +
                      "SET pg_yaap.memory_soft_limit='1MB';\n"
                      '\\set ON_ERROR_STOP off\n'
                      "SET statement_timeout='100ms';\n" + slow + '\n'
                      "SET statement_timeout='60s';\n"
                      '\\set ON_ERROR_STOP on\n' + recovery)
        result = runtime.run(cancel_sql, timeout=120)
        (OUT / 'cancel-reuse.out').write_text(result.stdout)
        (OUT / 'cancel-reuse.err').write_text(result.stderr)
        ok = (result.returncode == 0 and 'statement timeout' in result.stderr.lower()
              and result.stdout == expected_recovery)
        results.append({'case': 'cancel-reuse', 'pass': ok})
        print(json.dumps(results[-1]), flush=True)
        if not ok:
            raise AssertionError(f'RIGHT join cancellation/reuse failed; inspect {OUT}')
    finally:
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
