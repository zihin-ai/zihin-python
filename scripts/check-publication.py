"""Validate both build artifacts before uploading or publishing them."""
import re
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULES = {'__init__.py', '_client.py', '_errors.py', '_http.py', '_invoke.py',
           '_security.py', '_sse.py', '_triggers.py', '_types.py', 'py.typed'}
LANGUAGE = re.compile(r'\b(?:n\u00e3o|voc\u00ea|invoca\u00e7\u00e3o|configura\u00e7\u00e3o|usu\u00e1rio)\b', re.I)

def check():
    manifest = (ROOT / 'pyproject.toml').read_text()
    version = re.search(r'^version = "([^"]+)"$', manifest, re.M).group(1)
    dist = ROOT / 'dist'
    expected = {'zihin-' + version + '.tar.gz', 'zihin-' + version + '-py3-none-any.whl'}
    actual = {p.name for p in dist.iterdir() if p.is_file()}
    if actual != expected:
        raise ValueError('dist must contain exactly the current wheel and source archive')
    for name in sorted(expected):
        archive = dist / name
        if name.endswith('.whl'):
            prefix = 'zihin-' + version + '.dist-info/'
            allowed = {'zihin/' + module for module in MODULES} | {
                prefix + item for item in ['METADATA', 'WHEEL', 'RECORD', 'licenses/LICENSE']}
            with zipfile.ZipFile(archive) as z:
                entries = [(item.filename, z.read(item)) for item in z.infolist() if not item.is_dir()]
        else:
            prefix = 'zihin-' + version + '/'
            allowed = {'src/zihin/' + module for module in MODULES} | {'pyproject.toml', 'README.md', 'LICENSE', 'PKG-INFO', '.gitignore'}
            entries = []
            with tarfile.open(archive) as t:
                for item in t:
                    if item.isdir(): continue
                    if not item.isfile() or not item.name.startswith(prefix):
                        raise ValueError('Unexpected source archive entry')
                    entries.append((item.name[len(prefix):], t.extractfile(item).read()))
        names = [entry[0] for entry in entries]
        required = allowed - {'.gitignore'}
        if len(names) != len(set(names)) or set(names) - allowed or required - set(names):
            raise ValueError('Unexpected or missing files in ' + name)
        for path, value in entries:
            if LANGUAGE.search(value.decode('utf-8')):
                raise ValueError('Non-English text requires review: ' + path)
        print('Publication contents verified: ' + name)

if __name__ == '__main__':
    check()
