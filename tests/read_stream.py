#!/usr/bin/env python3
"""Check YAAP's PG ReadStream scan results and ringless BufferAccessStrategy."""
import json
import os

import memory_runtime as runtime

OUT = runtime.OUT
SCHEMA = f'yaap_read_stream_{os.getpid()}'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    table = f'{SCHEMA}.a'
    runtime.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {table} AS SELECT '
                'i::bigint AS id,(i%97)::int AS k, '
                "(repeat('x',128)||i::text)::text AS payload "
                'FROM generate_series(1,60000) i; '
                f'DELETE FROM {table} WHERE id%11=0; ANALYZE {table};')
    queries = {
        'scan': f'SELECT count(*),sum(id),sum(id*id) FROM {table};',
        'filter': f'SELECT k,count(*),sum(id) FROM {table} WHERE id>30000 GROUP BY k ORDER BY k;',
        'limit': f'SELECT id FROM {table} ORDER BY id LIMIT 7;',
    }
    try:
        for workers in (1, 4):
            for name, query in queries.items():
                expected = runtime.checked('SET pg_yaap.enabled=off;' + query)
                config = runtime.setup(workers, '512MB')
                result = runtime.run(config + query)
                tag = f'{name}-w{workers}'
                (OUT/f'{tag}.out').write_text(result.stdout)
                (OUT/f'{tag}.err').write_text(result.stderr)
                assert result.returncode == 0, (tag, result.stderr)
                assert result.stdout == expected, tag
                results.append({'case': tag, 'pass': True})
                print(json.dumps(results[-1]), flush=True)
    finally:
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT/'read-stream-summary.json').write_text(json.dumps(results, indent=2)+'\n')


if __name__ == '__main__':
    main()
