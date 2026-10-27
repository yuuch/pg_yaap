#!/usr/bin/env python3
"""Bounded, opt-in macOS stack evidence; never an I/O event counter."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys

MAX_FILE_BYTES = 20 * 1024 * 1024
FRAME = re.compile(r'^([ +!|:]*?)(\d+)\s+(.+?)(?:\s+\(in |\s+\[)')


def parse_sample(text):
    """Match ancestry within the call graph, not unrelated symbol mentions."""
    found = {'submitter_sync_read': [], 'buffer_io_wait': [],
             'buffer_replacement': [], 'io_worker_read': []}
    stack = []
    in_graph = False
    for line in text.splitlines():
        if line.strip() == 'Call graph:':
            in_graph = True
            continue
        if in_graph and (line.startswith('Total number in stack') or
                         line.startswith('Sort by top') or line.startswith('Binary Images:')):
            break
        if not in_graph:
            continue
        if re.match(r'^\s*\d+ Thread_', line):
            stack = []
            continue
        match = FRAME.match(line)
        if not match:
            continue
        depth = len(match[1])
        symbol = match[3]
        while stack and stack[-1][0] >= depth:
            stack.pop()
        stack.append((depth, symbol))
        names = [s for _, s in stack]
        yaap = any('YaapReadStream' in s or 'QueueRefill' in s or
                   'PhysicalSeqScan::' in s for s in names)
        def ordered(parts):
            cursor = 0
            for name in names:
                if parts[cursor] in name:
                    cursor += 1
                    if cursor == len(parts):
                        return True
            return False
        category = None
        if yaap and ordered(('pgaio_worker_submit', 'pgaio_io_perform_synchronously', 'pread')):
            category = 'submitter_sync_read'
        elif ordered(('IoWorkerMain', 'pgaio_io_perform_synchronously', 'pread')):
            category = 'io_worker_read'
        elif yaap and ordered(('WaitReadBuffers', 'pgaio_io_wait')) and any(
                s in symbol for s in ('ConditionVariable', 'WaitLatch', 'kevent', 'WaitEventSetWait')):
            category = 'buffer_io_wait'
        elif yaap and 'GetVictimBuffer' in symbol:
            category = 'buffer_replacement'
        if category and len(found[category]) < 3:
            found[category].append(names[-32:])
    return {
        'source': 'OS_STACK_SAMPLE',
        'semantic': 'SAMPLED_PRESENCE_NOT_EVENT_COUNT_OR_TIME',
        'call_graph_found': in_graph,
        'observed': {key: bool(value) for key, value in found.items()},
        'evidence_paths': found,
        'queue_full_fallback_count': None,
        'queue_full_fallback_availability': 'UNAVAILABLE',
        'limitations': [
            'A missing stack does not prove a path never ran.',
            'A pread/preadv call may hit OS page cache; it is not proof of physical SSD I/O.',
            'No synchronous-read frequency or duration can be derived from these presence flags.',
            'Sample attachment perturbs execution; do not use sampled query timing as the baseline.'
        ]
    }


def parse_file(path):
    if path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError(f'sample file exceeds {MAX_FILE_BYTES} bytes: {path}')
    return {'file': str(path.resolve()), **parse_sample(path.read_text(errors='replace'))}


def self_test():
    def graph(lines):
        return 'Call graph:\n' + '\n'.join(lines) + '\nBinary Images:\n'
    sync = ['  4 pg_yaap::pipeline::YaapReadStreamNext(x)  (in pg_yaap)',
            '    4 pgaio_worker_submit  (in postgres)',
            '      4 pgaio_io_perform_synchronously  (in postgres)',
            '        4 preadv  (in kernel)']
    assert parse_sample(graph(sync))['observed']['submitter_sync_read']
    siblings = sync[:2] + ['    4 unrelated  (in postgres)'] + sync[2:]
    assert not parse_sample(graph(siblings))['observed']['submitter_sync_read']
    assert not parse_sample('\n'.join(sync))['observed']['submitter_sync_read']
    no_yaap = sync[1:]
    assert not parse_sample(graph(no_yaap))['observed']['submitter_sync_read']
    wait = sync[:1] + ['    3 WaitReadBuffers  (in postgres)',
                      '      3 pgaio_io_wait  (in postgres)',
                      '        3 WaitLatch  (in postgres)']
    assert parse_sample(graph(wait))['observed']['buffer_io_wait']
    split_threads = sync[:2] + ['  3 Thread_2'] + sync[2:]
    assert not parse_sample(graph(split_threads))['observed']['submitter_sync_read']
    io = ['  1 IoWorkerMain  (in postgres)',
          '    1 pgaio_io_perform_synchronously  (in postgres)',
          '      1 preadv  (in kernel)']
    assert parse_sample(graph(io))['observed']['io_worker_read']
    assert not parse_sample(graph(io))['observed']['submitter_sync_read']
    print('8 stack ancestry checks passed')


def verify_pid(pid):
    # Explicit targets avoid guessing ownership among concurrent queries.
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'comm='],
                            text=True, capture_output=True, timeout=5)
    command = result.stdout.strip()
    if result.returncode or not command or 'postgres' not in Path(command).name:
        raise ValueError(f'PID {pid} is not a live postgres process: {command!r}')
    return command


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pid', type=int, action='append', default=[],
                        help='Explicit PostgreSQL worker PID; repeat up to 8 times')
    parser.add_argument('--parse-files', type=Path, nargs='+', default=[])
    parser.add_argument('--target-role', choices=('yaap', 'io'), default='yaap',
                        help='Caller-provided role; retained as metadata, not inferred')
    parser.add_argument('--output', type=Path, default=Path('artifacts/scan-io-stacks'))
    parser.add_argument('--duration', type=int, choices=range(1, 6), default=1)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    if len(args.pid) > 8 or len(set(args.pid)) != len(args.pid) or any(p <= 0 for p in args.pid):
        parser.error('supply 1–8 distinct positive PIDs')
    if len(args.parse_files) > 64:
        parser.error('at most 64 existing sample files per run')
    if not args.pid and not args.parse_files:
        if args.self_test:
            return
        parser.error('supply --pid or --parse-files')
    if args.pid and sys.platform != 'darwin':
        parser.error('live sampling requires macOS /usr/bin/sample')
    args.output.mkdir(parents=True, exist_ok=True)
    records = [parse_file(p) for p in args.parse_files]
    for pid in args.pid:
        command = verify_pid(pid)
        target = (args.output / f'worker-{pid}.sample.txt').resolve()
        if target.exists():
            raise ValueError(f'refusing to overwrite existing raw sample: {target}')
        result = subprocess.run(['/usr/bin/sample', str(pid), str(args.duration),
                                 '10', '-file', str(target)], text=True,
                                capture_output=True, timeout=args.duration + 10)
        (args.output / f'worker-{pid}.sampler.log').write_text(result.stdout + result.stderr)
        if result.returncode or not target.exists():
            raise RuntimeError(f'sampling PID {pid} failed: {result.stderr.strip()}')
        records.append({'pid': pid, 'target_role': args.target_role, 'process_command_at_attach': command, **parse_file(target)})
    report = {'schema_version': 1, 'samples': records,
              'sample_count': len(records),
              'query_worker_ownership': 'EXPLICIT_PIDS_SELECTED_BY_CALLER',
              'physical_disk_io': 'UNAVAILABLE'}
    (args.output / 'stack-evidence.json').write_text(json.dumps(report, indent=2) + '\n')
    for record in records:
        print(json.dumps({'file': record['file'], **record['observed']}))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(str(exc))
