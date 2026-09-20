"""Inspect existing runtime proposals; no new Spark execution, model calls, or tuning."""
from pathlib import Path
import json
ROOT=Path(__file__).resolve().parent
pairs=[json.loads(x) for x in (ROOT/'observer_check_v2'/'boundary_pairs.jsonl').read_text().splitlines()]
rows=[json.loads(x) for x in (ROOT/'observer_check_v2'/'raw.jsonl').read_text().splitlines()]
bytag={r['run_id']:r for r in rows}
report=[]
for case in ('observer_selective','observer_skew'):
    ps=[p for p in pairs if bytag[p['run_id']]['case']==case]
    distinct=[]
    for p in ps:
        current=[op for op in ('SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin') if op in p['current']['remaining_physical_plan']]
        proposed=[op for op in ('SortMergeJoin','BroadcastHashJoin','ShuffledHashJoin') if op in p['proposed']['remaining_physical_plan']]
        item={'current_joins':current,'proposed_joins':proposed,'plans_equal':p['plans_equal'],'native_would_choose':p['native_would_choose']}
        if item not in distinct:distinct.append(item)
    report.append({'case':case,'pairs':len(ps),'changed_pairs':sum(not p['plans_equal'] for p in ps),
                   'distinct_comparisons':distinct,'broadcast_available_in_either_remaining_plan':any('BroadcastHashJoin' in p[k]['remaining_physical_plan'] for p in ps for k in ('current','proposed'))})
out={'observed_candidate_coverage':report,
     'input_distribution_caveat':{'development_skew_seed313_keep500':'k=0 survives dimension filter because 313<500',
       'live_skew_seed733_keep500':'k=0 is filtered out because 733>=500; not a surviving-skew runtime example. Retain original data and do not relabel it as retained skew.'},
     'interpretation':'Fixed current/proposed advice cannot select a broadcast plan absent from both candidates. Whole-query broadcast headroom does not establish the legality or remaining-time gain of constructing that option at this boundary.'}
(ROOT/'CANDIDATE_COVERAGE.json').write_text(json.dumps(out,indent=2));print(json.dumps(out,indent=2))
