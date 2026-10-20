#!/usr/bin/env python3
"""Release policy shared by automation, dry runs, and publishing regression tests."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?')
TARGETS = {
    'x86_64-unknown-linux-gnu': 'linux',
    'aarch64-apple-darwin': 'macos',
    'x86_64-apple-darwin': 'macos',
    'x86_64-pc-windows-msvc': 'windows',
}


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def api(path, *, missing=False, fields=None):
    command = ['gh', 'api', path]
    if fields is not None:
        command += ['--method', 'POST']
        for key, value in fields.items():
            command += ['-f', f'{key}={value}']
    result = subprocess.run(command, capture_output=True, text=True)
    data = json.loads(result.stdout) if result.stdout.strip() else None
    if result.returncode:
        if missing and isinstance(data, dict) and str(data.get('status')) == '404':
            return None
        raise RuntimeError(f'GitHub API failed for {path}: {result.stderr.strip()}')
    return data


def package_version(root=ROOT):
    version = tomllib.loads((root / 'Cargo.toml').read_text())['package']['version']
    if not VERSION.fullmatch(version):
        raise ValueError('Cargo package version must be a release version')
    return version


def outputs(**values):
    with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
        for key, value in values.items():
            stream.write(f'{key}={value}\n')


def inspect_release(repo, commit):
    """Only a merged, same-repository release PR may request automatic publication."""
    requests = api(f'repos/{repo}/commits/{commit}/pulls')
    matches = [pr for pr in requests if pr.get('merged_at')
               and pr.get('merge_commit_sha') == commit and pr['base']['ref'] == 'main'
               and (pr['head'].get('repo') or {}).get('full_name') == repo
               and any(label['name'] == 'autorelease: pending' for label in pr['labels'])]
    if len(matches) > 1:
        raise ValueError('Ambiguous release PR for this commit')
    version = package_version()
    manifest = json.loads((ROOT / '.release-please-manifest.json').read_text())
    if manifest.get('.') != version:
        raise ValueError('Release manifest and Cargo.toml must agree')
    # One-time bootstrap; subsequent versions are inferred from Conventional Commits.
    outputs(release=str(bool(matches)).lower(), pr=matches[0]['number'] if matches else '',
            tag='v' + version, bootstrap='0.2.0' if version == '0.1.7' else '')


def prepare(tag):
    version = package_version()
    if tag != 'v' + version:
        raise ValueError('Tag must match Cargo.toml version')
    run('cargo', 'metadata', '--locked', '--no-deps', '--format-version=1')
    return run('git', 'rev-parse', 'HEAD')


def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def archive_binary(archive, target):
    expected = 'oxide.exe' if TARGETS[target] == 'windows' else 'oxide'
    with tarfile.open(archive) as source:
        members = source.getmembers()
        if len(members) != 1 or members[0].name != expected or not members[0].isfile():
            raise ValueError(f'Unexpected package contents: {archive.name}')
        return source.extractfile(members[0]).read()


def verify(directory, version, commit, report_path=None):
    archives = sorted(directory.glob('*.tar.gz'))
    files, platforms, hashes = [], set(), {}
    for archive in archives:
        prefix = f'oxide_{version}_'
        if not archive.name.startswith(prefix):
            raise ValueError('Unexpected package version')
        target = archive.name[len(prefix):-len('.tar.gz')]
        platform = TARGETS.get(target)
        if platform is None or platform in platforms:
            raise ValueError('Unexpected or duplicate package platform')
        platforms.add(platform)
        checksum = Path(str(archive) + '.sha256sum')
        expected, filename = checksum.read_text().strip().split(maxsplit=1)
        if filename.removeprefix('*') != archive.name or sha256(archive) != expected:
            raise ValueError('Package checksum mismatch')
        binary = archive_binary(archive, target)
        hashes[platform] = {'archive': expected, 'binary': hashlib.sha256(binary).hexdigest()}
        files.extend([archive, checksum])
    if platforms != {'linux', 'macos', 'windows'} or set(directory.iterdir()) != set(files):
        raise ValueError('Expected exactly three native packages and their checksums')
    if report_path:
        report = json.loads(report_path.read_text())
        manifest_path = ROOT / 'tests/upstream/manifest.json'
        manifest = json.loads(manifest_path.read_text())
        required = sum(test['tier'] == 'required' for test in manifest['tests'])
        if not (required > 0 and report.get('gate_passed') and not report.get('errors')
                and report.get('checkout_sha') == commit
                and report.get('manifest_sha256') == sha256(manifest_path)
                and report.get('required_executed') == required
                and report.get('counts') == {'passed': required, 'compatibility_failure': 0,
                                            'reference_failure': 0, 'harness_error': 0}
                and report.get('archive_sha256') == hashes['linux']['archive']
                and report.get('binary_sha256') == hashes['linux']['binary']
                and report.get('candidate', {}).get('oxideVersion') == version):
            raise ValueError('Linux package is not the exact successfully tested candidate')
    return files


def remote_tag(repo, tag):
    reference = api(f'repos/{repo}/git/ref/tags/{tag}', missing=True)
    if reference is None:
        return None
    obj = reference['object']
    for _ in range(10):
        if obj['type'] == 'commit':
            return obj['sha']
        if obj['type'] != 'tag':
            break
        obj = api(f'repos/{repo}/git/tags/{obj["sha"]}')['object']
    raise ValueError('Release tag does not resolve to a commit')


def release_notes(version, root=ROOT):
    text = (root / 'CHANGELOG.md').read_text()
    heading = re.search(r'^## \[?' + re.escape(version) + r'(?:\]|\s|$).*$', text, re.MULTILINE)
    if not heading:
        raise ValueError('Version is missing from CHANGELOG.md')
    rest = text[heading.end():]
    notes = re.split(r'^## ', rest, maxsplit=1, flags=re.MULTILINE)[0].strip()
    if not notes:
        raise ValueError('Release notes must not be empty')
    migration = root / 'docs' / 'release-notes' / f'{version}.md'
    if migration.exists():
        notes += '\n\n' + migration.read_text().strip()
    return notes


def publish(repo, tag, commit, files, notes, pr=None):
    """Called only after gates pass and under the per-tag publication lock."""
    tagged = remote_tag(repo, tag)
    if tagged is not None and tagged != commit:
        raise ValueError('Remote tag points to a different commit')
    release_path = f'repos/{repo}/releases/tags/{tag}'
    release = api(release_path, missing=True)
    expected = {path.name for path in files}
    if release is not None:
        if not release['draft']:
            raise ValueError('Refusing to replace an already published release')
        if {asset['name'] for asset in release['assets']} - expected:
            raise ValueError('Draft contains unverified assets; remove them before retrying')
    if tagged is None:
        api(f'repos/{repo}/git/refs', fields={'ref': 'refs/tags/' + tag, 'sha': commit})
    with tempfile.TemporaryDirectory(prefix='oxide-release-') as temporary:
        note_file = Path(temporary) / 'notes.md'
        note_file.write_text(notes + '\n')
        if release is None:
            subprocess.run(['gh', 'release', 'create', tag, '--repo', repo, '--draft',
                            '--verify-tag', '--title', tag, '--notes-file', str(note_file)], check=True)
        # Fetch fresh state before writes, and refuse manual publication/tag changes.
        release = api(release_path)
        if not release['draft'] or remote_tag(repo, tag) != commit:
            raise ValueError('Release or tag changed during publication')
        subprocess.run(['gh', 'release', 'upload', tag, '--repo', repo,
                        *map(str, files), '--clobber'], check=True)
        uploaded = api(release_path)
        if not uploaded['draft'] or {asset['name'] for asset in uploaded['assets']} != expected:
            raise ValueError('Draft asset set changed during publication')
        if remote_tag(repo, tag) != commit:
            raise ValueError('Release tag moved before publication')
        subprocess.run(['gh', 'release', 'edit', tag, '--repo', repo, '--draft=false',
                        '--prerelease=' + str('-' in tag).lower(), '--title', tag,
                        '--notes-file', str(note_file)], check=True)
    if pr:
        subprocess.run(['gh', 'pr', 'edit', str(pr), '--repo', repo,
                        '--remove-label', 'autorelease: pending', '--add-label', 'autorelease: tagged'], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['inspect', 'prepare', 'verify', 'publish', 'validate-title'])
    parser.add_argument('--repo', default=os.environ.get('GITHUB_REPOSITORY'))
    parser.add_argument('--tag')
    parser.add_argument('--commit')
    parser.add_argument('--pr')
    parser.add_argument('--directory', type=Path, default=ROOT / 'dist')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    if args.command == 'validate-title':
        if not re.fullmatch(r'(feat|fix|deps|docs|chore|build|ci|test|refactor|perf|revert)(\([^\r\n)]+\))?!?: [^\r\n]+', os.environ['PR_TITLE']):
            raise ValueError('Use a Conventional Commit PR title, such as feat: add a feature')
    elif args.command == 'inspect':
        inspect_release(args.repo, args.commit)
    elif args.command == 'prepare':
        outputs(tag=args.tag, commit=prepare(args.tag))
    else:
        if run('git', 'rev-parse', 'HEAD') != args.commit:
            raise ValueError('Unexpected validation checkout')
        version = package_version()
        if args.tag != 'v' + version:
            raise ValueError('Tag must match Cargo.toml version')
        files = verify(args.directory, version, args.commit, args.report)
        notes = release_notes(version)
        if args.command == 'publish':
            if args.report is None:
                raise ValueError('Publication requires the upstream report')
            publish(args.repo, args.tag, args.commit, files, notes, args.pr)
        print('Verified all three native packages for ' + args.commit)


if __name__ == '__main__':
    main()
