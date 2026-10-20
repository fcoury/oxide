#!/usr/bin/env python3
"""Exercise the executable users will extract from a native release archive."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile

spec = importlib.util.spec_from_file_location('release', Path(__file__).with_name('release.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
version = release.package_version()
archives = list((release.ROOT / 'dist').glob('*.tar.gz'))
if len(archives) != 1:
    raise ValueError('Expected exactly one native package')
archive = archives[0]
target = archive.name.removeprefix(f'oxide_{version}_').removesuffix('.tar.gz')
binary = release.archive_binary(archive, target)
with tempfile.TemporaryDirectory(prefix='oxide-package-') as temporary:
    executable = Path(temporary) / ('oxide.exe' if release.TARGETS[target] == 'windows' else 'oxide')
    executable.write_bytes(binary)
    executable.chmod(0o700)
    result = subprocess.check_output([str(executable), '--version'], text=True, timeout=30)
    if result.strip() != 'oxide ' + version:
        raise ValueError('Packaged version differs from Cargo.toml')
    result = subprocess.check_output([str(executable), 'shell', '--eval',
                                     'NumberLong("9223372036854775807")'], text=True, timeout=30)
    if '9223372036854775807' not in result:
        raise ValueError('Packaged shell did not preserve BSON Int64')
print('Native package version and embedded shell smoke passed: ' + archive.name)
