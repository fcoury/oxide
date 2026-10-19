#!/usr/bin/env python3
"""Run pinned, unmodified MongoDB JavaScript tests with the stock mongo shell.

Requires Docker, an isolated MongoDB reference, and either an isolated Oxide
endpoint or a candidate binary/archive plus a disposable PostgreSQL database.
No upstream assertions or shell helpers are replaced. JSON and JUnit reports
are written even when setup fails. See docs/upstream-tests.md.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
BEGIN = '__OXIDE_UPSTREAM_BEGIN__'
PASS = '__OXIDE_UPSTREAM_PASS__'


def digest(path):
    with Path(path).open('rb') as source:
        value = hashlib.sha256()
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
        return value.hexdigest()


def read_manifest(path):
    manifest = json.loads(path.read_text())
    if manifest.get('schema') != 1:
        raise ValueError('unsupported manifest schema')
    if not re.fullmatch('[a-f0-9]{40}', manifest['revision']):
        raise ValueError('upstream revision must be a commit SHA')
    if not re.fullmatch('[a-f0-9]{64}', manifest['archive_sha256']):
        raise ValueError('upstream archive needs a SHA256')
    if not re.fullmatch(r'mongo@sha256:[a-f0-9]{64}', manifest['shell_image']):
        raise ValueError('shell image must be pinned by digest')
    paths = []
    for item in manifest['tests']:
        path = item['path']
        if not path.startswith('jstests/') or not path.endswith('.js') or '..' in Path(path).parts:
            raise ValueError('invalid upstream test path')
        if item['tier'] not in ('required', 'extended', 'excluded') or not item.get('reason'):
            raise ValueError('each test needs a tier and reason')
        paths.append(path)
    if len(paths) != len(set(paths)) or not any(t['tier'] == 'required' for t in manifest['tests']):
        raise ValueError('manifest needs unique paths and a nonempty required baseline')
    return manifest


def endpoint(uri):
    value = urllib.parse.urlsplit(uri)
    if value.scheme != 'mongodb' or value.hostname not in ('127.0.0.1', 'localhost'):
        raise ValueError('only single-host loopback MongoDB endpoints are allowed')
    if value.username or value.password or value.path not in ('', '/'):
        raise ValueError('supply a credential-free endpoint without a database')
    value.port  # Reject malformed ports.
    options = urllib.parse.parse_qs(value.query)
    options.update(directConnection=['true'], retryWrites=['false'])
    return value._replace(query=urllib.parse.urlencode(options, doseq=True))


def fetch_archive(manifest, cache):
    cache.mkdir(parents=True, exist_ok=True)
    archive = cache / (manifest['revision'] + '.tar.gz')
    if not archive.exists():
        url = 'https://codeload.github.com/mongodb/mongo/tar.gz/' + manifest['revision']
        temporary = archive.with_suffix('.download-' + uuid.uuid4().hex)
        try:
            with urllib.request.urlopen(url, timeout=120) as source, temporary.open('wb') as dest:
                shutil.copyfileobj(source, dest)
            if digest(temporary) != manifest['archive_sha256']:
                raise ValueError('downloaded upstream archive checksum mismatch')
            temporary.replace(archive)
        finally:
            temporary.unlink(missing_ok=True)
    if digest(archive) != manifest['archive_sha256']:
        raise ValueError('cached upstream archive checksum mismatch; remove it and fetch again')
    return archive


def extract_source(archive, revision, dest):
    # Fresh extraction from the verified archive also verifies imported helper files.
    prefix = 'mongo-' + revision + '/'
    with tarfile.open(archive) as source:
        for member in source:
            if member.name == prefix[:-1] and member.isdir():
                continue
            if not member.name.startswith(prefix):
                raise ValueError('unexpected upstream archive root')
            relative = member.name[len(prefix):]
            if not relative or (not relative.startswith('jstests/') and '/' in relative):
                continue
            if '..' in Path(relative).parts or Path(relative).is_absolute():
                raise ValueError('unsafe upstream archive member')
            if member.isfile():
                target = dest / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as src, target.open('wb') as out:
                    shutil.copyfileobj(src, out)
    return dest


class Shell:
    def __init__(self, image, source, timeout):
        self.image, self.source, self.timeout = image, source, timeout

    def run(self, uri, code):
        name = 'oxide-upstream-' + uuid.uuid4().hex
        command = ['docker', 'run', '--rm', '--name', name, '--network', 'host',
                   '--read-only', '--cap-drop=ALL', '--security-opt=no-new-privileges',
                   '--tmpfs', '/tmp:rw,nosuid,size=64m', '-v', str(self.source) + ':/upstream:ro',
                   '-w', '/upstream', '--entrypoint', 'mongo', self.image,
                   '--quiet', '--norc', uri, '--eval', code]
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, timeout=self.timeout)
            return result.returncode, result.stdout
        except subprocess.TimeoutExpired as error:
            subprocess.run(['docker', 'rm', '-f', name], capture_output=True)
            raise RuntimeError('shell timed out; its container was removed') from error

    def identity(self, uri):
        code = 'var r=db.adminCommand({buildInfo:1});assert.commandWorked(r);print("__IDENTITY__"+JSON.stringify({version:r.version,oxideVersion:r.oxideVersion}));'
        status, output = self.run(uri, code)
        lines = [line.removeprefix('__IDENTITY__') for line in output.splitlines() if line.startswith('__IDENTITY__')]
        if status or len(lines) != 1:
            raise RuntimeError('endpoint preflight failed: ' + output[-2000:])
        return json.loads(lines[0])


def test_case(shell, uri, path, logfile):
    code = ('const cleanupDatabase=db; print(' + json.dumps(BEGIN) + ');try {load(' +
            json.dumps(path) + ');} finally {var cleanupResult=cleanupDatabase.runCommand({dropDatabase:1,writeConcern:{w:1}});if(cleanupResult.ok!==1)print("__UPSTREAM_CLEANUP_FAILED__");assert.commandWorked(cleanupResult);}' +
            'print(' + json.dumps(PASS) + ');')
    start = time.monotonic()
    status, output = shell.run(uri, code)
    logfile.write_text(output)
    lines = output.splitlines()
    if '__UPSTREAM_CLEANUP_FAILED__' in lines or BEGIN not in lines or (status == 0 and PASS not in lines):
        raise RuntimeError('test did not execute to completion; see ' + logfile.name)
    return {'passed': status == 0 and PASS in lines, 'exit_code': status,
            'seconds': round(time.monotonic() - start, 3), 'log': logfile.name,
            **({'error_excerpt': output[-2000:]} if status else {})}


def start_candidate(args, temporary, report):
    if args.candidate:
        return endpoint(args.candidate), None
    binary = args.oxide_binary
    if args.oxide_archive:
        archive = args.oxide_archive.resolve()
        report['archive_sha256'] = digest(archive)
        with tarfile.open(archive) as source:
            members = source.getmembers()
            if len(members) != 1 or members[0].name != 'oxide' or not members[0].isfile():
                raise ValueError('candidate archive must contain only the oxide executable')
            binary = temporary / 'oxide'
            with source.extractfile(members[0]) as src, binary.open('wb') as dest:
                shutil.copyfileobj(src, dest)
            binary.chmod(0o700)
    if not args.postgres_url:
        raise ValueError('a candidate binary requires --postgres-url')
    pg = urllib.parse.urlsplit(args.postgres_url)
    if pg.scheme not in ('postgres', 'postgresql') or pg.hostname not in ('127.0.0.1', 'localhost') or not pg.path.startswith('/oxide_upstream'):
        raise ValueError('PostgreSQL must be loopback and use an oxide_upstream* disposable database')
    binary = binary.resolve()
    report['binary_sha256'] = digest(binary)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    log = (args.report_dir / 'oxide.log').open('w')
    process = subprocess.Popen([str(binary), '--listen-addr', '127.0.0.1', '--port', str(port),
                                '--postgres-url', args.postgres_url], stdout=log, stderr=subprocess.STDOUT,
                               env={**os.environ, 'RUST_LOG': 'warn'})
    log.close()
    try:
        deadline = time.monotonic() + 30
        while process.poll() is None and time.monotonic() < deadline:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                    return endpoint(f'mongodb://127.0.0.1:{port}/'), process
            except OSError:
                time.sleep(0.1)
        raise RuntimeError('candidate did not start; see oxide.log')
    except Exception:
        process.terminate()
        process.wait(timeout=5)
        raise


def write_reports(report, directory):
    report['counts'] = {status: sum(row.get('status') == status for row in report['tests']) for status in ('passed', 'compatibility_failure', 'reference_failure', 'harness_error')}
    report['required_executed'] = sum(row['tier'] == 'required' for row in report['tests'])
    (directory / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    summary = ['# Upstream compatibility', '', 'Baseline gate: **' + ('PASS' if report['gate_passed'] else 'FAIL') + '**', '', 'Mode: `' + report['mode'] + '`', '', 'Required cases executed: ' + str(report['required_executed']), '', 'Outcomes: `' + json.dumps(report['counts']) + '`', '', 'Harness errors: ' + str(len(report['errors'])), '', 'Excluded cases: ' + str(len(report.get('excluded', []))), '', 'Extended compatibility failures are discovery results, not passing cases. See results.json and per-endpoint logs.']
    (directory / 'summary.md').write_text('\n'.join(summary) + '\n')
    suite = ET.Element('testsuite', name='upstream-mongodb')
    for row in report['tests']:
        case = ET.SubElement(suite, 'testcase', name=row['path'], time=str(row.get('seconds', 0)))
        if row.get('status', 'harness_error') != 'passed':
            ET.SubElement(case, 'failure', type=row.get('status', 'harness_error')).text = json.dumps({key: row[key] for key in ('error', 'reference', 'candidate') if key in row})
    for error in report['errors']:
        case = ET.SubElement(suite, 'testcase', name='harness')
        ET.SubElement(case, 'error', type='harness_error').text = error
    suite.set('tests', str(len(report['tests']) + len(report['errors'])))
    suite.set('failures', str(sum(r.get('status') != 'passed' for r in report['tests'])))
    suite.set('errors', str(len(report['errors'])))
    ET.ElementTree(suite).write(directory / 'junit.xml', encoding='utf-8', xml_declaration=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, default=ROOT / 'tests/upstream/manifest.json')
    parser.add_argument('--cache', type=Path, default=ROOT / 'target/upstream-cache')
    parser.add_argument('--mode', choices=['baseline', 'discovery'], default='baseline')
    parser.add_argument('--reference', required=True)
    candidates = parser.add_mutually_exclusive_group(required=True)
    candidates.add_argument('--candidate')
    candidates.add_argument('--oxide-binary', type=Path)
    candidates.add_argument('--oxide-archive', type=Path)
    parser.add_argument('--postgres-url', default=os.environ.get('UPSTREAM_POSTGRES_URL'))
    parser.add_argument('--report-dir', type=Path, required=True)
    parser.add_argument('--timeout', type=int, default=30)
    args = parser.parse_args()
    args.report_dir.mkdir(parents=True, exist_ok=True)
    report = {'mode': args.mode, 'tests': [], 'errors': [], 'gate_passed': False}
    process = None
    try:
        report['checkout_sha'] = subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip()
        if args.timeout < 1:
            raise ValueError('timeout must be positive')
        manifest = read_manifest(args.manifest)
        report.update(upstream_revision=manifest['revision'], upstream_version=manifest['version'],
                      shell_image=manifest['shell_image'], manifest_sha256=digest(args.manifest),
                      excluded=[t for t in manifest['tests'] if t['tier'] == 'excluded'])
        selected = [t for t in manifest['tests'] if t['tier'] == 'required' or (args.mode == 'discovery' and t['tier'] == 'extended')]
        reference = endpoint(args.reference)
        archive = fetch_archive(manifest, args.cache)
        if subprocess.run(['docker', 'image', 'inspect', manifest['shell_image']], capture_output=True).returncode:
            subprocess.run(['docker', 'pull', manifest['shell_image']], check=True, timeout=180)
        version = subprocess.check_output(['docker', 'run', '--rm', '--entrypoint', 'mongo', manifest['shell_image'], '--version'], text=True, timeout=30)
        if version.splitlines()[0] != 'MongoDB shell version v' + manifest['version']:
            raise ValueError('shell version differs from the pinned corpus')
        report['shell_version'] = version.splitlines()[0]
        with tempfile.TemporaryDirectory(prefix='oxide-upstream-') as name:
            temporary = Path(name)
            source = extract_source(archive, manifest['revision'], temporary / 'source')
            for test in selected:
                if not (source / test['path']).is_file():
                    raise ValueError('missing selected upstream test: ' + test['path'])
            shell = Shell(manifest['shell_image'], source, args.timeout)
            candidate, process = start_candidate(args, temporary, report)
            report['reference'] = shell.identity(reference.geturl())
            report['candidate'] = shell.identity(candidate.geturl())
            if report['reference'].get('oxideVersion') or not report['candidate'].get('oxideVersion'):
                raise ValueError('expected MongoDB reference and Oxide candidate, respectively')
            if report['reference']['version'] != manifest['reference_version']:
                raise ValueError('reference version differs from the manifest')
            for index, test in enumerate(selected):
                row = {**test, 'sha256': digest(source / test['path'])}
                report['tests'].append(row)
                db = 'oxide_upstream_' + uuid.uuid4().hex
                for lane, uri in [('reference', reference), ('candidate', candidate)]:
                    result = test_case(shell, uri._replace(path='/' + db).geturl(), test['path'], args.report_dir / f'{index:03}-{lane}.log')
                    row[lane] = result
                    if not result['passed']:
                        break
                row['status'] = ('reference_failure' if not row['reference']['passed'] else
                                 'passed' if row['candidate']['passed'] else 'compatibility_failure')
                row['seconds'] = sum(row.get(lane, {}).get('seconds', 0) for lane in ('reference', 'candidate'))
                print(row['status'] + ': ' + test['path'], flush=True)
            report['gate_passed'] = bool(report['tests']) and all(
                t['status'] == 'passed' or (t['tier'] == 'extended' and t['status'] == 'compatibility_failure')
                for t in report['tests'])
    except Exception as error:
        if report['tests'] and 'status' not in report['tests'][-1]:
            report['tests'][-1].update(status='harness_error', error=str(error))
        report['errors'].append(str(error))
        print('harness_error: ' + str(error), file=sys.stderr)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        report['gate_passed'] = report['gate_passed'] and not report['errors']
        write_reports(report, args.report_dir)
    print(json.dumps({'gate_passed': report['gate_passed'], 'executed': len(report['tests']), 'errors': len(report['errors'])}))
    return 0 if report['gate_passed'] else 2 if report['errors'] else 1


if __name__ == '__main__':
    sys.exit(main())
