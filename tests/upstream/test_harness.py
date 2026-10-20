"""Failure-path regressions for the upstream compatibility/release gate."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('upstream', Path(__file__).resolve().parents[2] / 'scripts/upstream-compat.py')
upstream = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(upstream)
MANIFEST = Path(__file__).with_name('manifest.json')


class GateFailures(unittest.TestCase):
    def test_empty_manifest_writes_failed_json_and_junit_without_using_docker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = json.loads(MANIFEST.read_text())
            manifest['tests'] = []
            (root / 'manifest.json').write_text(json.dumps(manifest))
            args = ['upstream-compat.py', '--manifest', str(root / 'manifest.json'), '--reference', 'mongodb://127.0.0.1:1/', '--candidate', 'mongodb://127.0.0.1:2/', '--report-dir', str(root / 'report')]
            original_run = upstream.subprocess.run
            def no_docker(command, **kwargs):
                if command[0] == 'docker':
                    raise AssertionError('must not invoke Docker')
                return original_run(command, **kwargs)
            with patch('sys.argv', args), patch.object(upstream.subprocess, 'run', side_effect=no_docker):
                self.assertEqual(upstream.main(), 2)
            report = json.loads((root / 'report/results.json').read_text())
            self.assertFalse(report['gate_passed'])
            self.assertTrue(report['errors'])
            self.assertIn('<error ', (root / 'report/junit.xml').read_text())

    def test_corrupt_cached_source_is_rejected_before_extraction(self):
        manifest = json.loads(MANIFEST.read_text())
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory)
            (cache / (manifest['revision'] + '.tar.gz')).write_bytes(b'corrupted')
            with patch.object(upstream.urllib.request, 'urlopen', side_effect=AssertionError('must not fetch a replacement silently')):
                with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                    upstream.fetch_archive(manifest, cache)

    def test_exit_zero_without_completion_and_failed_cleanup_are_harness_errors(self):
        class Shell:
            def __init__(self, output):
                self.output = output
            def run(self, *args):
                return 0, self.output
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / 'shell.log'
            for output in [upstream.BEGIN, upstream.BEGIN + '\n__UPSTREAM_CLEANUP_FAILED__\n' + upstream.PASS]:
                with self.assertRaisesRegex(RuntimeError, 'did not execute'):
                    upstream.test_case(Shell(output), 'unused', 'test.js', log)

    def test_source_archive_cannot_write_outside_its_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / 'source.tar.gz'
            with tarfile.open(archive, 'w:gz') as tar:
                entry = tarfile.TarInfo('mongo-' + 'a' * 40 + '/jstests/../../escape')
                entry.size = 1
                tar.addfile(entry, io.BytesIO(b'x'))
            with self.assertRaisesRegex(ValueError, 'unsafe upstream'):
                upstream.extract_source(archive, 'a' * 40, root / 'source')
            self.assertFalse((root / 'escape').exists())

    def test_unknown_tiers_and_duplicate_paths_are_not_silent_skips(self):
        for kind in ['unknown', 'duplicate']:
            data = json.loads(MANIFEST.read_text())
            if kind == 'unknown':
                data['tests'][0]['tier'] = 'unknown'
            else:
                data['tests'].append(data['tests'][0])
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'manifest.json'
                path.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    upstream.read_manifest(path)

    def test_timed_out_shell_container_is_removed(self):
        import subprocess
        shell = upstream.Shell('image', Path('/source'), 1)
        with patch.object(upstream.subprocess, 'run', side_effect=[subprocess.TimeoutExpired('docker', 1), subprocess.CompletedProcess('docker', 0)]) as run:
            with self.assertRaisesRegex(RuntimeError, 'timed out'):
                shell.run('mongodb://127.0.0.1/', 'code')
        cleanup = run.call_args_list[1].args[0]
        self.assertEqual(cleanup[:3], ['docker', 'rm', '-f'])
        self.assertTrue(cleanup[3].startswith('oxide-upstream-'))

    def test_only_local_credential_free_base_endpoints_are_allowed(self):
        for uri in ['mongodb://example.com/', 'mongodb://a:b@127.0.0.1/', 'mongodb://127.0.0.1/production']:
            with self.assertRaises(ValueError):
                upstream.endpoint(uri)


if __name__ == '__main__':
    unittest.main()
