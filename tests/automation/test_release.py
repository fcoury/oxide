"""Release-side effects are mocked: these tests never create tags or releases."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('release', Path(__file__).resolve().parents[2] / 'scripts/release.py')
r = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r)


class Artifacts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.dist = self.root / 'dist'
        self.dist.mkdir()
        self.version = '0.2.0'
        for target in ['x86_64-unknown-linux-gnu', 'aarch64-apple-darwin', 'x86_64-pc-windows-msvc']:
            self.archive(target)

    def archive(self, target, member=None):
        archive = self.dist / f'oxide_{self.version}_{target}.tar.gz'
        binary = b'fixture native executable for ' + target.encode()
        member = member or ('oxide.exe' if 'windows' in target else 'oxide')
        with tarfile.open(archive, 'w:gz') as output:
            info = tarfile.TarInfo(member)
            info.size = len(binary)
            output.addfile(info, io.BytesIO(binary))
        Path(str(archive) + '.sha256sum').write_text(r.sha256(archive) + '  ' + archive.name + '\n')
        return archive

    def report(self):
        manifest = r.ROOT / 'tests/upstream/manifest.json'
        required = sum(t['tier'] == 'required' for t in json.loads(manifest.read_text())['tests'])
        linux = next(self.dist.glob('*linux*.tar.gz'))
        binary = r.archive_binary(linux, 'x86_64-unknown-linux-gnu')
        data = {'gate_passed': True, 'errors': [], 'checkout_sha': 'tested-sha',
                'manifest_sha256': r.sha256(manifest), 'required_executed': required,
                'counts': {'passed': required, 'compatibility_failure': 0, 'reference_failure': 0, 'harness_error': 0},
                'archive_sha256': r.sha256(linux), 'binary_sha256': r.hashlib.sha256(binary).hexdigest(),
                'candidate': {'oxideVersion': self.version}}
        report = self.root / 'results.json'
        report.write_text(json.dumps(data))
        return report, data

    def test_all_platforms_and_exact_tested_linux_package_are_required(self):
        report, data = self.report()
        self.assertEqual(len(r.verify(self.dist, self.version, 'tested-sha', report)), 6)
        for field, invalid in [('archive_sha256', 'other'), ('binary_sha256', 'other'),
                               ('checkout_sha', 'other'), ('gate_passed', False),
                               ('required_executed', 0), ('manifest_sha256', 'other')]:
            with self.subTest(field=field):
                report.write_text(json.dumps({**data, field: invalid}))
                with self.assertRaises(ValueError):
                    r.verify(self.dist, self.version, 'tested-sha', report)

    def test_missing_duplicate_or_extra_assets_cannot_pass(self):
        linux = next(self.dist.glob('*linux*.tar.gz'))
        for extra in [self.dist / 'unexpected.txt', self.dist / 'oxide_0.2.0_x86_64-apple-darwin.tar.gz']:
            extra.write_bytes(linux.read_bytes())
            with self.assertRaises((ValueError, FileNotFoundError)):
                r.verify(self.dist, self.version, 'sha')
            extra.unlink()
        linux.unlink()
        with self.assertRaises(ValueError):
            r.verify(self.dist, self.version, 'sha')

    def test_corruption_and_archive_traversal_are_rejected(self):
        linux = next(self.dist.glob('*linux*.tar.gz'))
        linux.write_bytes(b'corrupt')
        with self.assertRaises(ValueError):
            r.verify(self.dist, self.version, 'sha')
        self.archive('x86_64-unknown-linux-gnu', '../oxide')
        with self.assertRaises(ValueError):
            r.verify(self.dist, self.version, 'sha')

    def test_binary_checksum_notation_is_accepted(self):
        for checksum in self.dist.glob('*.sha256sum'):
            checksum.write_text(checksum.read_text().replace('  ', ' *'))
        self.assertEqual(len(r.verify(self.dist, self.version, 'sha')), 6)


class Publishing(unittest.TestCase):
    def setUp(self):
        self.files = [Path('dist/oxide.tar.gz'), Path('dist/oxide.tar.gz.sha256sum')]
        self.repo, self.tag, self.commit = 'owner/repo', 'v0.2.0', 'tested-sha'
        self.calls = []
        self.draft = {'draft': True, 'assets': [{'name': p.name} for p in self.files]}

    def call(self, tag=None, states=None):
        states = list(states if states is not None else [self.draft] * 3)
        def api(path, **kwargs):
            if path.endswith('/git/refs'):
                self.calls.append(('tag', kwargs))
                return {}
            return states.pop(0)
        with patch.object(r, 'remote_tag', return_value=self.commit if tag is None else tag), \
                patch.object(r, 'api', side_effect=api), \
                patch.object(r.subprocess, 'run', side_effect=lambda args, **kw: self.calls.append(args)):
            r.publish(self.repo, self.tag, self.commit, self.files, 'notes')

    def test_published_release_or_unverified_draft_assets_block_writes(self):
        for state in [{'draft': False, 'assets': []}, {'draft': True, 'assets': [{'name': 'old.tar.gz'}]}]:
            with self.subTest(state=state):
                self.calls.clear()
                with self.assertRaises(ValueError):
                    self.call(states=[state])
                self.assertEqual(self.calls, [])

    def test_tag_mismatch_and_manual_publication_are_rechecked(self):
        with self.assertRaises(ValueError):
            self.call(tag='different-sha')
        self.assertEqual(self.calls, [])
        with self.assertRaises(ValueError):
            self.call(states=[self.draft, {'draft': False, 'assets': []}])
        self.assertEqual(self.calls, [])

    def test_stable_and_prerelease_flags_are_explicit_for_existing_drafts(self):
        for tag, flag in [('v0.2.0', '--prerelease=false'), ('v0.2.0-rc.1', '--prerelease=true')]:
            self.tag = tag
            self.calls.clear()
            self.call()
            edit = next(args for args in self.calls if args[:3] == ['gh', 'release', 'edit'])
            self.assertIn(flag, edit)
            self.assertIn('--draft=false', edit)

    def test_new_release_starts_as_a_draft_then_uploads_then_publishes(self):
        with patch.object(r, 'remote_tag', side_effect=[None, self.commit, self.commit]), \
                patch.object(r, 'api', side_effect=[None, {}, self.draft, self.draft]) as api, \
                patch.object(r.subprocess, 'run') as run:
            r.publish(self.repo, self.tag, self.commit, self.files, 'notes')
        self.assertIn('--draft', run.call_args_list[0].args[0])
        self.assertEqual([call.args[0][2] for call in run.call_args_list], ['create', 'upload', 'edit'])
        self.assertEqual(api.call_args_list[1].kwargs['fields']['sha'], self.commit)

    def test_network_errors_are_not_treated_as_a_missing_release(self):
        for response in [(1, '{"status":"403"}'), (1, '')]:
            with patch.object(r.subprocess, 'run', return_value=type('Result', (), {
                'returncode': response[0], 'stdout': response[1], 'stderr': 'failure'})()):
                with self.assertRaises(RuntimeError):
                    r.api('repos/owner/repo/releases/tags/v0.2.0', missing=True)
        with patch.object(r.subprocess, 'run', return_value=type('Result', (), {
            'returncode': 1, 'stdout': '{"status":"404"}', 'stderr': 'missing'})()):
            self.assertIsNone(r.api('missing', missing=True))


class ReleaseRequest(unittest.TestCase):
    def test_only_a_merged_same_repo_release_pr_for_exact_commit_can_publish(self):
        valid = {'number': 99, 'merged_at': 'now', 'merge_commit_sha': 'sha',
                 'base': {'ref': 'main'}, 'head': {'repo': {'full_name': 'owner/repo'}},
                 'labels': [{'name': 'autorelease: pending'}]}
        for change, expected in [({}, 'true'), ({'merged_at': None}, 'false'),
                                 ({'merge_commit_sha': 'other'}, 'false'),
                                 ({'labels': []}, 'false'),
                                 ({'head': {'repo': {'full_name': 'fork/repo'}}}, 'false')]:
            with self.subTest(change=change), patch.object(r, 'api', return_value=[{**valid, **change}]), \
                    patch.object(r, 'package_version', return_value='0.1.7'), patch.object(r, 'outputs') as output:
                r.inspect_release('owner/repo', 'sha')
                self.assertEqual(output.call_args.kwargs['release'], expected)
                self.assertEqual(output.call_args.kwargs['bootstrap'], '0.2.0')

    def test_bot_and_normal_pr_titles_pass_but_nonconventional_titles_fail(self):
        for title in ['chore(main): release 0.2.0', 'feat!: automate releases', 'fix: repair matching']:
            with patch.dict(r.os.environ, {'PR_TITLE': title}), patch('sys.argv', ['release.py', 'validate-title']):
                r.main()
        for title in ['Add release automation', 'feat: title\nextra line']:
            with patch.dict(r.os.environ, {'PR_TITLE': title}), patch('sys.argv', ['release.py', 'validate-title']):
                with self.assertRaises(ValueError):
                    r.main()

    def test_release_notes_include_migration_and_exclude_previous_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'CHANGELOG.md').write_text('# Changelog\n\n## [0.2.0](link) (date)\n\nNew changes\n\n## [0.1.7]\n\nOld changes\n')
            notes = root / 'docs/release-notes'
            notes.mkdir(parents=True)
            (notes / '0.2.0.md').write_text('Storage migration required')
            result = r.release_notes('0.2.0', root)
            self.assertIn('New changes', result)
            self.assertIn('Storage migration required', result)
            self.assertNotIn('Old changes', result)
            with self.assertRaises(ValueError):
                r.release_notes('0.2.1', root)


if __name__ == '__main__':
    unittest.main()
