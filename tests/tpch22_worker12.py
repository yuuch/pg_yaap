#!/usr/bin/env python3
"""Run every TPC-H query once with a 12-worker/8GB YAAP configuration."""
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ.get('YAAP_TPCH_OUTPUT', ROOT / 'artifacts/tpch22-final-w12-8gb-20261019'))
OUT.mkdir(parents=True, exist_ok=True)
QUERIES = Path(os.environ.get('QUERY_DIR', '/Users/chenyunwen/proj/postgres_19dev/contrib/pg_carbon/tests/tpch'))
PSQL = os.environ.get('PSQL', '/Users/chenyunwen/proj/postgres/installed-bench/bin/psql')
LIBRARY = Path(os.environ.get('YAAP_LIBRARY', ROOT / 'build-bench/pg_yaap.dylib')).resolve()
BASE = [PSQL, '-X', '-U', os.environ.get('PGUSER', 'postgres'), '-h', os.environ.get('PGHOST', '/tmp'), '-p', os.environ.get('PGPORT', '55432'), '-d', os.environ.get('PGDATABASE', 'tpch'), '-v', 'ON_ERROR_STOP=1', '-P', 'pager=off']
SETUP = (f"LOAD '{str(LIBRARY)}'; SET pg_yaap.enabled=on; SET pg_yaap.parallel=on; "
         "SET pg_yaap.parallel_max_workers=12; SET pg_yaap.memory_limit='8GB'; "
         "SET pg_yaap.memory_soft_limit=-1; SET statement_timeout='180s'; "
         "SET pg_yaap.trace_execution_path=on; \\timing on\n")


def call(sql, timeout=220):
    return subprocess.run(BASE, input=sql, capture_output=True, text=True, timeout=timeout)


def main():
    results = []
    for n in range(1, 23):
        source = QUERIES / f'q{n}.sql'
        sql = SETUP + source.read_text()
        if n == 15:
            states = call("SET pg_yaap.enabled=off; SELECT evtname||':'||evtenabled::text FROM pg_event_trigger WHERE evtname LIKE 'duckdb_%';")
            if states.returncode:
                raise RuntimeError(states.stderr)
            disables, restores = [], []
            for line in states.stdout.splitlines():
                line = line.strip()
                if ':' not in line: continue
                name, mode = line.rsplit(':', 1)
                name, mode = name.strip(), mode.strip()
                identifier = '"' + name.replace('"', '""') + '"'
                disables.append(f'ALTER EVENT TRIGGER {identifier} DISABLE;\n')
                restores.append(f'ALTER EVENT TRIGGER {identifier} ' + {'O':'ENABLE','D':'DISABLE','R':'ENABLE REPLICA','A':'ENABLE ALWAYS'}[mode] + ';\n')
            sql = (SETUP + 'BEGIN;\n' + ''.join(disables) + source.read_text()
                   + '\nROLLBACK;\n')
        (OUT / f'q{n}.sql').write_text(sql)
        started = time.monotonic()
        try:
            result = call(sql)
            elapsed = round(time.monotonic() - started, 3)
            (OUT / f'q{n}.out').write_text(result.stdout)
            (OUT / f'q{n}.err').write_text(result.stderr)
            status = 'PASS' if result.returncode == 0 else 'FAIL'
            diagnostic = result.stderr.strip().splitlines()[:2]
        except subprocess.TimeoutExpired as exc:
            elapsed = round(time.monotonic() - started, 3)
            (OUT / f'q{n}.out').write_bytes(exc.stdout or b'')
            (OUT / f'q{n}.err').write_bytes(exc.stderr or b'')
            status, diagnostic = 'TIMEOUT', [f'client watchdog exceeded 220s ({exc.timeout}s statement timeout also set)']
        row = {'query':n,'status':status,'wall_seconds':elapsed,'diagnostic':diagnostic}
        results.append(row)
        (OUT / 'summary.json').write_text(json.dumps(results, indent=2)+'\n')
        print(json.dumps(row,ensure_ascii=False),flush=True)
        # A backend/worker crash makes postmaster recovery asynchronous.
        if status != 'PASS':
            for _ in range(60):
                probe=call('SET pg_yaap.enabled=off; SELECT 1;',timeout=5)
                if probe.returncode==0:break
                time.sleep(1)
            else:raise RuntimeError('PostgreSQL did not recover after query failure')
    raise SystemExit(int(any(r['status']!='PASS' for r in results)))

if __name__=='__main__':main()
