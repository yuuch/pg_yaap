#!/usr/bin/env python3
"""Validate bounded Block ownership/read/Wait evidence without changing scan results."""
import json
import os
import re
import time
from pathlib import Path
from collections import defaultdict

import memory_runtime as runtime

OUT = runtime.OUT
SCHEMA = f'yaap_scan_blocks_{os.getpid()}'
LOG = Path(os.environ['YAAP_BLOCK_TRACE_LOG']) if os.environ.get('YAAP_BLOCK_TRACE_LOG') else None
FIELDS = re.compile(r'(\w+)=([^\s]+)')


def parse_trace(stderr):
    events, summaries = defaultdict(list), []
    for line in stderr.splitlines():
        if 'pg_yaap[scan-block-summary]' in line:
            summaries.append(dict(FIELDS.findall(line)))
        elif 'pg_yaap[scan-block]' in line:
            event = dict(FIELDS.findall(line))
            events[tuple(event[k] for k in ('pid', 'worker', 'rel', 'stream'))].append(event)
    return events, summaries


def validate_trace(stderr, capacity, pages, require_full=False):
    streams, summaries = parse_trace(stderr)
    assert summaries, 'enabled tracing must declare completeness/capacity'
    seen = set()
    all_needs = []
    for summary in summaries:
        key = tuple(summary[k] for k in ('pid', 'worker', 'rel', 'stream'))
        assert key not in seen, ('unexpected duplicate stream identity', key)
        seen.add(key)
        events = streams.get(key, [])
        assert int(summary['capacity']) == capacity
        assert int(summary['recorded']) == len(events) <= capacity
        assert int(summary['dropped']) >= 0
        assert summary['complete'] in ('yes', 'no')
        if require_full:
            assert int(summary['dropped']) == 0, summary
            assert summary['complete'] == 'yes', summary
        sequence = [int(e['seq']) for e in events]
        assert sequence == sorted(sequence) and len(sequence) == len(set(sequence))
        starts = {}
        waits = set()
        assigns = []
        needs = []
        for event in events:
            phase = event['event']
            assert phase in ('ASSIGN', 'START', 'NEED', 'WAIT', 'CLEANUP_WAIT'), event
            assert int(event['time_ns']) > 0
            assert int(event['end_ns']) >= int(event['time_ns']), event
            assert int(event['pages']) >= 1
            block = int(event['block'])
            assert pages > 0 and 0 <= block < pages, event
            if phase == 'START':
                request = int(event['request'])
                assert request > 0 and request not in starts
                assert 0 < int(event['pages']) <= int(event['requested'])
                starts[request] = event
            elif phase == 'ASSIGN':
                assert int(event['last_block']) == (block + int(event['pages']) - 1) % pages
                assigns.append(event)
            elif phase == 'NEED':
                assert int(event['pages']) == 1
                assert int(event['need_wait']) in (-1, 0, 1)
                needs.append(event)
            else:
                request = int(event['request'])
                assert request not in waits, ('request waited twice', key, request)
                waits.add(request)
                if request in starts:
                    start = starts[request]
                    assert event['block'] == start['block']
                    assert event['pages'] == start['pages']
                    assert int(event['time_ns']) >= int(start['end_ns'])
                elif require_full:
                    raise AssertionError(('Wait without recorded Start', event))
        if require_full:
            for need in needs:
                request = int(need['request'])
                assert request in starts, ('Need without Start', need)
                start = starts[request]
                block = int(need['block'])
                assert int(start['block']) <= block < int(start['block']) + int(start['pages'])
                assert any((block - int(a['block'])) % pages < int(a['pages']) for a in assigns), ('Need outside assigned ranges', need)
        all_needs.extend(needs)
    assert set(streams) <= seen, 'events without stream summary'
    return streams, summaries, all_needs


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    table = f'{SCHEMA}.a'
    runtime.ddl(f'CREATE SCHEMA {SCHEMA}; CREATE TABLE {table} AS SELECT '
                "i::bigint AS id,repeat('x',128)::text AS padding FROM generate_series(1,12000) i; "
                f'CREATE TABLE {SCHEMA}.empty AS SELECT * FROM {table} WHERE false; '
                f'ANALYZE {table}; ANALYZE {SCHEMA}.empty;')
    results = []
    try:
        pages = int(runtime.checked(f"SELECT pg_relation_size('{table}')/current_setting('block_size')::bigint;"))
        checksum = f'SELECT count(*),sum(id),sum(id*id) FROM {table};'
        cases = [
            ('off', checksum, 1, 0, 'off', pages, False),
            ('single', checksum, 1, 8192, 'off', pages, True),
            ('io-detail', checksum, 1, 8192, 'detail', pages, True),
            ('multi', checksum, 4, 8192, 'off', pages, True),
            ('overflow', checksum, 1, 8, 'off', pages, False),
            ('empty', f'SELECT id FROM {SCHEMA}.empty;', 1, 8192, 'off', 0, True),
            ('limit', f'SELECT 1 FROM {table} LIMIT 7;', 1, 8192, 'off', pages, True),
        ]
        for name, query, workers, cap, io_mode, relation_pages, full in cases:
            expected = runtime.checked('SET pg_yaap.enabled=off;' + query)
            config = runtime.setup(workers, '512MB') + (
                'SET pg_yaap.scan_read_stream=yaap; '
                'SET pg_yaap.scan_prefetch_buffers=128; '
                'SET pg_yaap.scan_prefetch_requests=4; '
                'SET pg_yaap.scan_prefetch_batch_pages=16; '
                'SET pg_yaap.profile=off; SET pg_yaap.profile_scan=off; '
                'SET pg_yaap.profile_timeline=off; '
                f'SET pg_yaap.profile_scan_io={io_mode}; '
                f'SET pg_yaap.profile_scan_blocks={cap}; ')
            log_offset = LOG.stat().st_size if LOG else None
            result = runtime.run(config + query)
            block_output = result.stderr
            if LOG:
                # Worker LOG messages are emitted to the server log, not the
                # leader's client connection. Allow the logger to flush after
                # query/worker completion, without changing executor timing.
                time.sleep(0.2)
                assert LOG.stat().st_size >= log_offset, 'server log rotated during test'
                with LOG.open('rb') as log:
                    log.seek(log_offset)
                    delta = log.read().decode(errors='replace')
                block_output = delta
                (OUT / f'{name}.blocks.log').write_text(delta)

            (OUT / f'{name}.out').write_text(result.stdout)
            (OUT / f'{name}.err').write_text(result.stderr)
            assert result.returncode == 0, (name, result.stderr)
            assert result.stdout.splitlines() == expected.splitlines(), name
            if cap == 0:
                assert 'pg_yaap[scan-block' not in block_output
            else:
                streams, summaries, needs = validate_trace(block_output, cap, relation_pages, full)
                if name in ('single', 'io-detail', 'multi'):
                    assert len(needs) == pages, (name, len(needs), pages)
                    assert sorted(int(e['block']) for e in needs) == list(range(pages)), name
                if name == 'overflow':
                    assert any(int(s['dropped']) > 0 for s in summaries)
                    assert any(int(s['recorded']) == cap for s in summaries)
                if name == 'empty':
                    assert not needs
                if name == 'limit':
                    assert needs
            results.append({'case': name, 'pass': True})
            (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')
            print(json.dumps(results[-1]), flush=True)
    finally:
        runtime.ddl(f'DROP SCHEMA {SCHEMA} CASCADE;')
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
