"""Read-only environment audit. Does not inspect secrets or call model APIs."""
import importlib.metadata as md
import importlib.util
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

workspace = Path.home() / 'JevResearch'
kit = Path.home() / 'jev-mac-native'

def check_path(p):
    return {'path': str(p), 'exists': p.exists(), 'is_directory': p.is_dir(),
            'readable': os.access(p, os.R_OK), 'writable': os.access(p, os.W_OK)}

def command(args):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=15)
        return {'returncode': p.returncode, 'output': (p.stdout + p.stderr)[:5000]}
    except Exception as e:
        return {'error': type(e).__name__}

packages = {}
for name in ('pyspark', 'py4j', 'mcp', 'pytest', 'numpy', 'scipy', 'pandas', 'httpx', 'requests', 'typesafe'):
    try:
        packages[name] = md.version(name)
    except md.PackageNotFoundError:
        packages[name] = None
report = {
    'paths': [check_path(p) for p in (workspace, kit, workspace/'jev_spark_research_lab.zip')],
    'python': sys.version, 'python_executable': sys.executable, 'prefix': sys.prefix,
    'platform': platform.platform(), 'machine': platform.machine(), 'cpu_count': os.cpu_count(),
    'packages': packages,
    'java_path': shutil.which('java'), 'java_version': command(['java', '-version']),
    'ram_bytes': command(['/usr/sbin/sysctl', '-n', 'hw.memsize']),
    'memory_pages': command(['/usr/bin/vm_stat']),
    'bridge_top_level_names': sorted(p.name for p in kit.iterdir() if not p.name.startswith('.')) if kit.is_dir() else [],
    'workspace_top_level_names': sorted(p.name for p in workspace.iterdir() if not p.name.startswith('.')),
    'pyspark_package_dir': str(Path(importlib.util.find_spec('pyspark').origin).parent) if importlib.util.find_spec('pyspark') else None,
    'jev_requests_attempted_by_this_script': 0,
}
output = Path(__file__).resolve().parent/'environment.json'
output.write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
