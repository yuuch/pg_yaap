#!/usr/bin/env python3
"""Compare correlated aggregate results with PostgreSQL on the same fixtures.

Requires a running dedicated PG instance; see memory_runtime.py for PG and
library environment variables. Uses a process-specific schema and removes it.
"""
import os
import memory_runtime as runtime

SCHEMA = f'yaap_delim_reuse_{os.getpid()}'


def main():
    outer = f'{SCHEMA}.outer_rows'
    inner = f'{SCHEMA}.inner_rows'
    empty = f'{SCHEMA}.empty_rows'
    runtime.ddl(f'CREATE SCHEMA {SCHEMA}; '
                f'CREATE TABLE {outer}(id int, k int, payload text); '
                f"INSERT INTO {outer} VALUES (1,1,'same'),(2,1,'same'),"
                "(3,2,'two'),(4,NULL,'null'),(5,3,'no match'),(6,1,'same'); "
                f'CREATE TABLE {inner}(k int, v int); '
                f'INSERT INTO {inner} VALUES (1,10),(1,10),(1,20),(2,5),'
                '(2,NULL),(NULL,1),(4,8); '
                f'CREATE TABLE {empty}(k int, v int); '
                f'ANALYZE {outer}; ANALYZE {inner}; ANALYZE {empty};')
    try:
        cases = {
            'min_ties_duplicates': f'SELECT o.id,o.k,o.payload,i.v FROM {outer} o '
                f'JOIN {inner} i ON i.k=o.k WHERE i.v=(SELECT min(x.v) FROM {inner} x '
                'WHERE x.k=o.k) ORDER BY o.id,i.v;',
            'scalar_null_and_missing': f'SELECT o.id,o.k,(SELECT min(x.v) FROM {inner} x '
                'WHERE x.k=o.k) AS m FROM ' + outer + ' o ORDER BY o.id;',
            'empty_inner': f'SELECT o.id,(SELECT min(x.v) FROM {empty} x WHERE x.k=o.k) '
                f'FROM {outer} o ORDER BY o.id;',
            'empty_outer': f'SELECT o.id,(SELECT min(x.v) FROM {inner} x WHERE x.k=o.k) '
                f'FROM {outer} o WHERE o.id < 0 ORDER BY o.id;',
        }
        for name, query in cases.items():
            expected = runtime.checked('SET pg_yaap.enabled=off; ' + query)
            for workers in (1, 4, 12):
                actual = runtime.run(runtime.setup(workers, '256MB') + query, timeout=120)
                if actual.returncode or actual.stdout != expected:
                    raise AssertionError(f'{name} workers={workers}: {actual.stderr}\n'
                                         f'expected={expected!r} actual={actual.stdout!r}')
                print(f'{name} workers={workers}: PASS', flush=True)
    finally:
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')


if __name__ == '__main__':
    main()
