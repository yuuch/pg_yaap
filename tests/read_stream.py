#!/usr/bin/env python3
"""A/B correctness and cleanup for the bounded YAAP PG-buffer read queue."""
import json
import os
from pathlib import Path
import re
import time

import memory_runtime as runtime
import pipeline_timeline as timeline

OUT = runtime.OUT
SCHEMA = f'yaap_read_stream_{os.getpid()}'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    results = []
    value = runtime.checked(runtime.setup(1,'512MB') +
                            'SET pg_yaap.enabled=off; SHOW pg_yaap.scan_prefetch_requests;')
    assert value.strip() == '1', value
    results.append({'case':'default-one-request','pass':True})
    table = f'{SCHEMA}.a'
    runtime.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {table} AS SELECT '
                'i::bigint AS id,(i%97)::int AS k,(i%100)::numeric(15,2) AS n,'
                "(repeat('x',128)||i::text)::text AS s FROM generate_series(1,60000) i; "
                f'DELETE FROM {table} WHERE id%11=0; '
                f'CREATE TABLE {SCHEMA}.b AS SELECT * FROM {table} WHERE id<20000; '
                f'CREATE TABLE {SCHEMA}.empty AS SELECT * FROM {table} WHERE false; '
                f'ANALYZE {table}; ANALYZE {SCHEMA}.b; ANALYZE {SCHEMA}.empty;')
    queries = {
        'checksum': f'SELECT count(*),sum(id),sum(id*id) FROM {table};',
        'filter': f'SELECT k,count(*),sum(id),sum(n) FROM {table} WHERE id>30000 GROUP BY k ORDER BY k;',
        'reject': f'SELECT id FROM {table} WHERE id<0;',
        'strings': f'SELECT id,s FROM {table} WHERE id<300 ORDER BY id;',
        'multi-source': f'SELECT a.k,count(*),sum(a.id) FROM {table} a JOIN {SCHEMA}.b b ON a.id=b.id GROUP BY a.k ORDER BY a.k;',
        'empty': f'SELECT id FROM {SCHEMA}.empty;',
        'limit': f'SELECT 1 FROM {table} LIMIT 7;',
    }
    try:
        pages = int(runtime.checked(f"SELECT pg_relation_size('{table}')/current_setting('block_size')::bigint;"))
        for name, query in queries.items():
            expected = runtime.checked('SET pg_yaap.enabled=off;' + query)
            for mode, workers, window, limit in [('pg',4,1024,0),('yaap',1,17,1),('yaap',4,128,2),('yaap',12,1024,4),('yaap',4,8192,1),('yaap',4,8192,0)]:
                tag = f'{name}-{mode}-w{workers}-b{window}-r{limit}'
                config = runtime.setup(workers,'512MB') + f'SET pg_yaap.scan_read_stream={mode}; SET pg_yaap.scan_prefetch_buffers={window}; SET pg_yaap.scan_prefetch_requests={limit}; SET pg_yaap.scan_prefetch_batch_pages={int(os.environ.get('YAAP_TEST_BATCH_PAGES','0'))}; SET pg_yaap.profile_scan=sample; '
                result = runtime.run(config + query)
                (OUT/f'{tag}.out').write_text(result.stdout);(OUT/f'{tag}.err').write_text(result.stderr)
                assert result.returncode == 0,(tag,result.stderr)
                assert sorted(result.stdout.splitlines()) == sorted(expected.splitlines()),tag
                stats=[]
                for line in result.stderr.splitlines():
                    if 'pg_yaap[read-stream] worker ' in line:
                        stats.append(dict(re.findall(r'(\w+)=([^\s]+)',line)))
                if mode == 'yaap':
                    cpu=[]
                    for line in result.stderr.splitlines():
                        if 'pg_yaap[read-stream-cpu] worker ' in line:
                            c=dict(re.findall(r'(\w+)=([^\s]+)',line));cpu.append(c)
                            assert float(c['submit_sample_cpu_ms']) <= float(c['submit_sample_wall_ms']) + .001
                            assert float(c['wait_sample_cpu_ms']) <= float(c['wait_sample_wall_ms']) + .001
                        if 'pg_yaap[read-stream-queue] worker ' in line:
                            c=dict(re.findall(r'(\w+)=([^\s]+)',line))
                            assert int(c['refills']) >= 0 and int(c['split_requests']) >= 0
                            assert int(c['request_limit']) == limit
                            if limit:
                                assert int(c['max_queue_inflight']) <= limit, (tag, c)
                    if name == 'checksum':
                        assert sum(int(c['submit_samples']) + int(c['cpu_clock_failures']) for c in cpu)>0
                    assert stats,tag
                    assert all(1<=int(s['window'])<=window and 1<=int(s['batch'])<=int(s['window']) for s in stats),tag
                    if name == 'checksum':
                        assert sum(int(s['accepted_pages']) for s in stats)==pages,(tag,stats,pages)
                else:
                    assert not stats,tag
                results.append({'case':tag,'pass':True})
                print(json.dumps(results[-1]),flush=True)
        if os.environ.get('YAAP_TEST_STRESS_SPLIT') == '1':
            # A relation larger than shared_buffers, with warm alternating
            # pages in its prefix, forces mixed hit/miss split continuations.
            big=f'{SCHEMA}.big'
            runtime.ddl(f'CREATE TABLE {big} AS SELECT i::bigint AS id,'
                        "repeat('z',600)::text AS pad FROM generate_series(1,250000) i; "
                        f'ANALYZE {big};')
            check=f'SELECT count(*),sum(id),sum(id*id) FROM {big};'
            native=runtime.checked('SET pg_yaap.enabled=off;'+check)
            tids=','.join(f"'({page},1)'::tid" for page in range(0,512,2))
            config=runtime.setup(1,'512MB')+"SET pg_yaap.scan_read_stream=yaap; SET pg_yaap.scan_prefetch_buffers=8192; SET pg_yaap.scan_prefetch_requests=1; SET pg_yaap.profile_scan=sample; "
            warm=f'SET pg_yaap.enabled=off; SELECT count(*) FROM {big} WHERE ctid IN ({tids}); SET pg_yaap.enabled=on; '
            result=runtime.run(config+warm+check,timeout=150)
            (OUT/'mixed-split.out').write_text(result.stdout);(OUT/'mixed-split.err').write_text(result.stderr)
            assert result.returncode==0 and result.stdout.splitlines()[1:]==native.splitlines(),result.stderr
            stats=[dict(re.findall(r'(\w+)=([^\s]+)',l)) for l in result.stderr.splitlines() if 'pg_yaap[read-stream-queue] worker ' in l]
            assert stats and sum(int(x['split_requests']) for x in stats)>0
            assert all(int(x['max_queue_inflight'])<=1 for x in stats)
            assert sum(int(x['request_limit_stops']) for x in stats)>0
            results.append({'case':'mixed-cache-split-quota-one','pass':True})
            result=runtime.run(config+warm+f'SELECT 1 FROM {big} LIMIT 7;'+check,timeout=150)
            (OUT/'mixed-limit.out').write_text(result.stdout);(OUT/'mixed-limit.err').write_text(result.stderr)
            assert result.returncode==0 and result.stdout.splitlines()[1:]==['1']*7+native.splitlines(),result.stderr
            results.append({'case':'mixed-unsubmitted-limit-reuse','pass':True})
        config=runtime.setup(4,'512MB')+'SET pg_yaap.scan_read_stream=yaap; SET pg_yaap.scan_prefetch_buffers=8192; '
        result=runtime.run(config+queries['limit']+queries['checksum'])
        expected=runtime.checked('SET pg_yaap.enabled=off;'+queries['limit']+queries['checksum'])
        assert result.returncode==0 and result.stdout==expected,result.stderr
        results.append({'case':'limit-same-backend-reuse','pass':True})
        # Reuse the real cancel/worker-termination injector, with this stream.
        timeline.SCHEMA=SCHEMA
        timeline.abnormal_checks(results,queries['multi-source'],scan_mode='sample',read_stream='yaap')
    finally:
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT/'summary.json').write_text(json.dumps(results,indent=2)+'\n')


if __name__=='__main__':
    main()
