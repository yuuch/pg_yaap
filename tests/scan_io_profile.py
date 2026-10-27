#!/usr/bin/env python3
"""Scan IO observations, availability, merge invariants and policy neutrality."""
import json
import os
from pathlib import Path
import re
import time
import pipeline_timeline as t

OUT=t.OUT
SCHEMA=f'yaap_scan_io_{os.getpid()}'
STRICT_UNAVAILABLE=('buffer_hit_rate','buffer_hit_pages','buffer_miss_pages',
 'buffer_already_loading_pages','prefetch_ready_rate','cold_prefetch_ready_rate',
 'actual_io_wait_ns','actual_io_wait_count','io_stall_ratio','io_completion_ns',
 'aio_queue_full_fallback_count','aio_forced_sync_count','sync_read_count','sync_read_ns')
ADDITIVE=('streams_created','consumed_pages','known_ready_at_need_pages','readiness_unknown_pages',
 'request_not_submitted_at_need_pages','submit_after_need_pages','api_lead_pages',
 'next_total_ns','wait_validation_calls','api_start_calls','api_accepted_pages',
 'start_ready_pages','start_need_wait_pages','pin_binding_events','request_binding_events',
 'window_full_events','slot_binding_events','validation_observations',
 'block_assignment_ns','pin_budget_check_ns','submit_cpu_samples','submit_sample_wall_ns',
 'submit_sample_cpu_ns','wait_cpu_samples','wait_sample_wall_ns','wait_sample_cpu_ns','cpu_clock_failures',
 'pg_start_read_ns','pg_wait_read_ns','pg_wait_new_read_pages','pg_start_timed_calls','pg_wait_timed_calls',
 'kernel_profiled_operations','kernel_requests','kernel_requested_bytes','kernel_completed_bytes',
 'kernel_queued_requests','kernel_worker_requests','kernel_queue_full_sync_requests',
 'kernel_forced_sync_requests','kernel_queue_ns','kernel_reopen_ns','kernel_read_ns',
 'kernel_shared_complete_ns','kernel_enqueue_to_shared_ns','kernel_timed_read_requests',
 'kernel_failed_requests','kernel_unclassified_requests','page_consumer_gap_ns','page_consumer_gap_count',
 'buffer_alloc_calls',
 'buffer_lookup_hits',
 'buffer_lookup_misses',
 'buffer_ring_attempts',
 'buffer_ring_hits',
 'buffer_freelist_candidates',
 'buffer_freelist_hits',
 'buffer_clock_candidates',
 'buffer_clock_usage_decrements',
 'buffer_clock_pinned_skips',
 'buffer_victim_retries',
 'buffer_dirty_writebacks',
 'buffer_profiled_start_calls',
 'buffer_sampled_start_calls',
 'buffer_sampled_pages',
 'buffer_alloc_total_ns',
 'buffer_lookup_ns',
 'buffer_mapping_lock_ns',
 'buffer_victim_ns',
 'buffer_strategy_ns',
 'buffer_ring_ns',
 'buffer_freelist_ns',
 'buffer_clock_ns',
 'buffer_dirty_write_ns',
 'buffer_invalidate_ns',
 'buffer_pin_ns')

def validate_io(data,full=True):
    t.validate(data,not full)
    scans=[]
    for pipeline in data['yaap']['pipelines']:
        local=[]
        for w in pipeline['workers']:
            io=w.get('scan_io')
            if not io:continue
            if not io.get('complete'):
                assert not data['yaap']['complete'];continue
            if io.get('read_stream')=='pg':
                assert io['custom_stream_observations']['value'] is None
                continue
            observed=io['observed'];local.append(io);scans.append(io)
            for name in STRICT_UNAVAILABLE:
                assert io['metrics'][name]['availability']=='UNAVAILABLE',name
                assert io['metrics'][name]['value'] is None,name
            assert observed['known_ready_at_need_pages']+observed['readiness_unknown_pages']==observed['consumed_pages']
            assert observed['api_lead_pages']+observed['submit_after_need_pages']==observed['consumed_pages']
            assert observed['start_ready_pages']+observed['start_need_wait_pages']==observed['api_accepted_pages']
            assert observed['consumed_pages']<=observed['api_accepted_pages']
            assert observed['next_total_ns']<=observed['source_ns']+1000
            assert observed['submit_sample_cpu_ns']<=observed['submit_sample_wall_ns']
            assert observed['wait_sample_cpu_ns']<=observed['wait_sample_wall_ns']
            assert observed['submit_cpu_samples']<=observed['api_start_calls']
            assert observed['wait_cpu_samples']<=observed['wait_validation_calls']
            assert observed['block_assignment_ns']+observed['pin_budget_check_ns']<=observed['refill_exclusive_ns']+1000
            for name, calls, timed, envelope in (
                    ('pg_start_read_ns', 'api_start_calls', 'pg_start_timed_calls', 'submit_api_ns'),
                    ('pg_wait_read_ns', 'wait_validation_calls', 'pg_wait_timed_calls', 'wait_api_ns')):
                assert observed[timed] <= observed[calls]
                # PG's bracket is inside the API; allow per-call clock quantization.
                assert observed[name] <= observed[envelope] + 1000 * observed[calls]
                metric = io['metrics'][name]
                available = observed[calls] > 0 and observed[timed] == observed[calls]
                assert metric['availability'] == ('EXACT' if available else 'UNAVAILABLE')
                assert metric['value'] == (observed[name] if available else None)
                assert metric['source'] == 'POSTGRES_STATISTICS'
                assert metric['semantic'] == 'PG_ACCOUNTED_READ_TIME_WITHIN_API'
                if not observed[timed]: assert observed[name] == 0
            assert observed['pg_wait_new_read_pages'] >= 0
            assert observed['kernel_profiled_operations'] <= observed['wait_validation_calls']
            assert observed['kernel_timed_read_requests'] <= observed['kernel_requests']
            assert observed['kernel_worker_requests'] <= observed['kernel_requests']
            assert observed['kernel_queued_requests'] <= observed['kernel_requests']
            assert observed['kernel_queue_full_sync_requests'] <= observed['kernel_requests']
            assert observed['kernel_forced_sync_requests'] <= observed['kernel_requests']
            assert observed['kernel_requests'] == sum(observed[k] for k in (
                'kernel_worker_requests','kernel_queue_full_sync_requests',
                'kernel_forced_sync_requests','kernel_unclassified_requests'))
            kernel = io['metrics']['kernel_timing']
            # The feature flag is build capability; only detail opts into kernel clocks.
            if kernel['availability'] == 'EXACT':
                assert kernel['value'] is True
                assert kernel['feature_version'] > 0
                assert observed['kernel_profiled_operations'] == observed['wait_validation_calls']
                assert observed['kernel_timed_read_requests'] == observed['kernel_requests']
                assert observed['kernel_failed_requests'] == observed['kernel_unclassified_requests'] == 0
            else:
                assert kernel['availability'] == 'UNAVAILABLE' and kernel['value'] is None
            if not io['config']['kernel_feature_supported']:
                assert kernel['availability'] == 'UNAVAILABLE'
                assert all(observed[k] == 0 for k in ADDITIVE if k.startswith('kernel_'))
            for name in ('kernel_operation_queue_ns','kernel_operation_read_ns'):
                assert io['histograms'][name]['count'] <= observed['kernel_profiled_operations']
            assert observed['buffer_alloc_calls'] == observed['buffer_lookup_hits'] + observed['buffer_lookup_misses']
            assert observed['buffer_ring_hits'] <= observed['buffer_ring_attempts']
            assert observed['buffer_freelist_hits'] <= observed['buffer_freelist_candidates']
            assert observed['buffer_sampled_pages'] <= observed['buffer_alloc_calls']
            assert observed['buffer_sampled_start_calls'] <= observed['buffer_profiled_start_calls'] <= observed['api_start_calls']
            buffer = io['metrics']['buffer_profile']
            # Detail selection is represented by capability and the profile's own declared availability.
            if buffer['availability'] == 'EXACT':
                assert buffer['value'] is True and io['config']['buffer_feature_supported']
                assert observed['buffer_profiled_start_calls'] == observed['api_start_calls']
            else:
                assert buffer['availability'] == 'UNAVAILABLE' and buffer['value'] is None
            sampled = io['metrics']['buffer_alloc_sampled_ns']
            if buffer['availability'] == 'EXACT' and observed['buffer_sampled_start_calls']:
                assert sampled['availability'] == 'ESTIMATED'
                assert sampled['value'] == observed['buffer_alloc_total_ns']
            else:
                assert sampled['availability'] == 'UNAVAILABLE' and sampled['value'] is None
            assert sampled['sample_rate'] == 64 and sampled['nested_phase_times_are_inclusive']
            if not io['config']['buffer_feature_supported']:
                assert all(observed[k] == 0 for k in ADDITIVE if k.startswith('buffer_'))
            assert observed['page_consumer_gap_count'] <= observed['consumed_pages']
            assert io['histograms']['page_consumer_gap_distribution_ns']['count'] == observed['page_consumer_gap_count']
            consumer = io['metrics']['page_consumer_gap_ns']
            assert consumer['availability'] == ('EXACT' if observed['page_consumer_gap_count'] else 'UNAVAILABLE')
            assert consumer['value'] == (observed['page_consumer_gap_ns'] if observed['page_consumer_gap_count'] else None)
            if os.environ.get('YAAP_TEST_EXPECT_PG_TIMING') == 'off':
                assert observed['pg_start_timed_calls'] == observed['pg_wait_timed_calls'] == 0
                for name in ('pg_start_read_ns','pg_wait_read_ns'):
                    assert io['metrics'][name]['availability'] == 'UNAVAILABLE'
                    assert io['metrics'][name]['value'] is None
            assert observed['streams_created']==1,'Task reentry must preserve a stream'
            for name,h in io['histograms'].items():
                if 'bins' not in h:continue
                assert len(h['bins'])==64 and sum(h['bins'])==h['count'],name
        agg=pipeline.get('scan_io')
        if local:
            assert agg
            for key in ADDITIVE:assert agg['observed'][key]==sum(x['observed'][key] for x in local),(key,pipeline['pipeline_id'])
    aggregate=data['yaap'].get('scan_io')
    if full and scans:
        assert aggregate
        for key in ADDITIVE:assert aggregate['observed'][key]==sum(x['observed'][key] for x in scans),key
        for name,h in aggregate['histograms'].items():
            if 'bins' not in h:continue
            assert h['bins']==[sum(x['histograms'][name]['bins'][i] for x in scans) for i in range(64)],name
    return scans

def main():
    OUT.mkdir(parents=True,exist_ok=True);t.SCHEMA=SCHEMA;results=[]
    a=f'{SCHEMA}.a'
    t.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {a} AS SELECT i::bigint AS id,(i%97)::int AS k,'
          "(i%100)::numeric(15,2) AS n,(repeat('x',128)||(i%19)::text)::text AS s FROM generate_series(1,300000) i; "
          f'DELETE FROM {a} WHERE id%31=0; CREATE TABLE {SCHEMA}.b AS SELECT * FROM {a} WHERE id<20000; '
          f'CREATE TABLE {SCHEMA}.empty AS SELECT * FROM {a} WHERE false; '
          f'ANALYZE {a}; ANALYZE {SCHEMA}.b; ANALYZE {SCHEMA}.empty;')
    sql={
      'checksum':f'SELECT count(*),sum(id),sum(id*id) FROM {a};',
      'filter':f'SELECT k,sum(id),sum(n) FROM {a} WHERE id<100000 GROUP BY k ORDER BY k;',
      'strings':f'SELECT s,count(*) FROM {a} WHERE id<1000 GROUP BY s ORDER BY s;',
      'empty':f'SELECT id FROM {SCHEMA}.empty;',
      'reject':f'SELECT id FROM {a} WHERE id<0;',
      'join':f'SELECT a.k,count(*) FROM {a} a JOIN {SCHEMA}.b b ON a.id=b.id GROUP BY a.k ORDER BY a.k;',
      'limit':f'SELECT 1 FROM {a} LIMIT 7;'}
    try:
        pages=int(t.checked(f"SELECT pg_relation_size('{a}')/current_setting('block_size')::bigint;"))
        cases=[('off','checksum',4,'off','pg',False,8192),
               ('summary','checksum',4,'summary','pg',False,8192),
               *[(f'workers-{w}','checksum',w,'detail','pg',False,8192) for w in (1,4,8,12)],
               ('filter','filter',4,'detail','pg',False,8192),
               ('strings','strings',4,'detail','pg',False,8192),
               ('empty','empty',4,'detail','pg',False,8192),
               ('reject','reject',4,'detail','pg',False,8192),
               ('join','join',4,'detail','pg',False,8192),
               ('limit','limit',4,'detail','pg',False,8192),
               ('native','checksum',4,'detail','pg',False,8192),
               ('legacy','checksum',4,'detail','pg',True,8192),
               ('overflow','checksum',4,'detail','pg',False,1)]
        for name,q,workers,mode,stream,legacy,cap in cases:
            expected=t.checked('SET pg_yaap.enabled=off;'+sql[q])
            prefix=OUT/f'{name}-{time.time_ns()}'
            config=t.setup(prefix,workers,'trace',cap,legacy,io_mode=mode)
            r=t.run(config+sql[q]);(OUT/f'{name}.out').write_text(r.stdout);(OUT/f'{name}.err').write_text(r.stderr)
            assert r.returncode==0,(name,r.stderr)
            assert sorted(r.stdout.splitlines())==sorted(expected.splitlines()),name
            files=list(OUT.glob(prefix.name+'.*.json'));assert len(files)==1
            data=json.loads(files[0].read_text())
            if mode=='off':assert 'pg_yaap[scan-io]' not in r.stderr
            else:
                scans=validate_io(data,cap!=1)
                assert 'actual_io_stall=UNAVAILABLE' in r.stderr
                assert 'code=QUEUE_FULL_FALLBACK status=CONFIRMED' not in r.stderr
                if stream=='yaap' and q in ('checksum','filter','strings','reject'):
                    assert sum(x['observed']['consumed_pages'] for x in scans)==pages,name
                if q=='empty':assert sum(x['observed']['consumed_pages'] for x in scans)==0
                if mode=='summary':
                    assert 'pg_yaap[scan-io-hist]' not in r.stderr
                    for x in scans:
                        assert x['metrics']['kernel_timing']['availability']=='UNAVAILABLE'
                        assert x['observed']['kernel_profiled_operations']==0
                        assert x['observed']['page_consumer_gap_count']==0
                        assert x['metrics']['buffer_profile']['availability']=='UNAVAILABLE'
                        assert all(x['observed'][k]==0 for k in ADDITIVE if k.startswith('buffer_'))
                if mode=='detail' and scans:
                    for x in scans:
                        o=x['observed'];h=x['histograms']
                        if x['config']['buffer_feature_supported'] and stream=='yaap':
                            assert x['metrics']['buffer_profile']['availability']=='EXACT'
                        if x['config']['kernel_feature_supported'] and stream=='yaap':
                            assert x['metrics']['kernel_timing']['availability']=='EXACT'
                            assert o['kernel_profiled_operations']==o['wait_validation_calls']
                        assert h['start_api_ns']['count']==o['api_start_calls']
                        assert h['wait_api_ns']['count']==o['wait_validation_calls']
                        assert h['accepted_pages_per_call']['count']==o['api_start_calls']
                        assert h['api_return_to_need_ns']['count']==o['api_lead_pages']
                        assert h['validated_ready_upper_ns']['count']==o['validation_observations']
            results.append({'case':name,'pass':True,'trace':str(files[0])})
            (OUT/'summary.json').write_text(json.dumps(results,indent=2));print(json.dumps(results[-1]),flush=True)
        r=t.run(t.setup(OUT/'standalone',mode='off',io_mode='summary')+sql['checksum'])
        (OUT/'standalone.err').write_text(r.stderr)
        assert r.returncode==0 and 'pg_yaap[scan-io] query ' in r.stderr
        assert 'pg_yaap[timing]' not in r.stderr and 'pg_yaap[scan] query' not in r.stderr
        results.append({'case':'independent-summary','pass':True})
        t.abnormal_checks(results,sql['join'],io_mode='detail')
    finally:
        t.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT/'summary.json').write_text(json.dumps(results,indent=2))

if __name__=='__main__':main()
