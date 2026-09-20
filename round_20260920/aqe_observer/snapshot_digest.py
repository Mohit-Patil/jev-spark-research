"""Read-only digest of captured synthetic AQE state, excluding any HTTP credentials."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parent
pairs=[json.loads(x) for x in (ROOT/'live_shadow_v2'/'callback_snapshots.jsonl').read_text().splitlines()]
out=[]
for p in pairs:
    r={'case':p['run_id'],'native_would_choose':p['native_would_choose'],'current':{},'proposed':{}}
    for view in ('current','proposed'):
        s=p[view]
        r[view]={'operators':[op for op in ('SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin') if op in s['remaining_physical_plan']],
                 'stages':[{k:x.get(k) for k in ('stage_id','node','materialized','row_count','size_in_bytes','partition_count','partition_bytes_total','partition_bytes_max','partition_bytes_p50')} for x in s['stages']]}
    out.append(r)
(ROOT/'SNAPSHOT_DIGEST.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
