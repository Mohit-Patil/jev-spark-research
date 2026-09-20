"""Inspect only expected secret-file metadata. Never read or emit secret contents."""
from pathlib import Path
import json, os, stat
root=Path.home()/'JevResearch'; kit=Path.home()/'jev-mac-native'
report=[]
for p in [root/'.env', root/'round_20260920'/'.env', kit/'.env']:
    r={'path':str(p),'exists':p.exists(),'symlink':p.is_symlink()}
    if p.exists():
        s=p.lstat(); r.update(mode=oct(stat.S_IMODE(s.st_mode)),owned_by_current_user=s.st_uid==os.getuid(),regular_file=stat.S_ISREG(s.st_mode),bytes=s.st_size)
    report.append(r)
print(json.dumps({'secret_files_metadata_only':report,'api_attempts':0},indent=2))
(root/'round_20260920'/'secret_metadata.json').write_text(json.dumps(report,indent=2))
