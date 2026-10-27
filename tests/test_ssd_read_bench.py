#!/usr/bin/env python3
"""Small correctness/CLI checks; these cached fixtures are not SSD measurements."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
BINARY=Path(os.environ.get('SSD_READ_BENCH',ROOT/'artifacts/ssd-read-20261025/ssd_read_bench'))

class ReadBenchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory(prefix='ssd-read-test-')
        cls.directory=Path(cls.tmp.name)
        for name,byte in [('123',17),('123.1',29)]:
            (cls.directory/name).write_bytes(bytes([byte])*(2*1024*1024))
        (cls.directory/'123_fsm').write_bytes(b'x'*8192)
        (cls.directory/'123.xyz').write_bytes(b'y'*8192)
        cls.before={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in cls.directory.iterdir()}
    @classmethod
    def tearDownClass(cls):
        after={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in cls.directory.iterdir()}
        if after!=cls.before:raise AssertionError('benchmark changed input files')
        cls.tmp.cleanup()
    def run_case(self,*extra):
        args=[str(BINARY),'--directory',str(self.directory),'--stem','123','--threads','2','--seconds','1','--warmup','0','--cache','buffered','--readahead','off',*extra]
        r=subprocess.run(args,text=True,capture_output=True,timeout=15)
        self.assertEqual(r.returncode,0,r.stderr)
        d=json.loads(r.stdout)
        self.assertEqual(d['block_bytes'],8192)
        self.assertEqual(d['dataset_bytes'],4*1024*1024)
        self.assertGreater(d['operations'],0)
        self.assertEqual(d['read_bytes'],d['operations']*d['request_bytes'])
        self.assertGreater(d['elapsed_seconds'],0)
        self.assertGreater(d['MiB_s'],0)
        self.assertGreater(d['cpu_seconds'],0)
        return d
    def test_sequential_8k(self):
        d=self.run_case('--pattern','seq','--pages-per-call','1','--layout','contiguous')
        self.assertEqual(d['request_bytes'],8192)
    def test_random_8k(self):
        d=self.run_case('--pattern','random','--pages-per-call','1','--layout','contiguous')
        self.assertEqual(d['request_bytes'],8192)
    def test_scatter_batch(self):
        d=self.run_case('--pattern','seq','--pages-per-call','128','--layout','scatter')
        self.assertEqual(d['request_bytes'],1024*1024)
    def test_staged_batch(self):
        d=self.run_case('--pattern','seq','--pages-per-call','128','--layout','staged')
        self.assertEqual(d['request_bytes'],1024*1024)
    def test_invalid_arguments(self):
        r=subprocess.run([str(BINARY),'--threads','0'],text=True,capture_output=True,timeout=5)
        self.assertNotEqual(r.returncode,0)
    def test_missing_dataset(self):
        r=subprocess.run([str(BINARY),'--directory',str(self.directory),'--stem','999','--seconds','1'],text=True,capture_output=True,timeout=5)
        self.assertNotEqual(r.returncode,0)

if __name__=='__main__':unittest.main()
