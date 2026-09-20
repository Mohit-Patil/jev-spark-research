"""Preregistered feature-summary ablation. Uses only a captured runtime event.
COMPACT omits full plan text and partition order, so this is not a lossless codec.
No outcome labels, query names, native cost labels, hints or saved times are used.
"""
import copy,json,math
from runtime_advisor_v5 import payload as verbose_payload

def canonical(obj):
    return json.loads(json.dumps(obj,sort_keys=True,separators=(',',':'),allow_nan=False))

def stage_features(s):
    if s.get('materialized') is not True or s.get('is_runtime') is not True:raise ValueError('unfinished_stage')
    rows=s['rows'];size=s['runtime_size_bytes'];xs=s['serialized_partition_bytes']
    if type(rows) is not int or type(size) is not int or rows<0 or size<0:raise ValueError('invalid_runtime_stat')
    if not isinstance(xs,list) or not 1<=len(xs)<=4096 or any(type(x) is not int or x<0 for x in xs):raise ValueError('invalid_partitions')
    a=sorted(xs);p50=a[math.ceil(len(a)*.5)-1];p95=a[math.ceil(len(a)*.95)-1]
    return {'runtime_rows':rows,'spark_runtime_size_bytes':size,'serialized_shuffle_bytes_total':sum(xs),
      'partitions':len(xs),'nonempty_partitions':sum(x>0 for x in xs),'partition_bytes_min':a[0],
      'partition_bytes_p50_nearest_rank':p50,'partition_bytes_p95_nearest_rank':p95,
      'partition_bytes_max':a[-1],'max_to_p50_ratio':a[-1]/p50 if p50 else None,
      'hash_table_memory_bytes':None}

def build(event,view):
    full=verbose_payload(event)
    if view=='VERBOSE':return canonical(full)
    if view!='COMPACT':raise ValueError('unknown_feature_view')
    initial=event['original_join'].splitlines()[0];candidate=event['candidate_join'].splitlines()[0]
    if not initial.startswith('SortMergeJoin') or not candidate.startswith('BroadcastHashJoin') or 'BuildRight' not in candidate:raise ValueError('unsupported_operators')
    if 'LeftOuter' in initial:join='LEFT_OUTER'
    elif 'Inner' in initial:join='INNER'
    else:raise ValueError('unsupported_join_semantics')
    full['state']={'engine':'Spark 4.0.1','boundary':'both input shuffles already materialized',
      'join':{'type':join,'key':'one Long equality key','residual_condition':None},
      'resources':full['state']['resources'],
      'left':stage_features(event['left']),'right':stage_features(event['right']),
      'candidates':{'NATIVE':{'algorithm':'sort_merge','reuse':'both existing shuffled inputs','additional_work':'sort inputs and merge join'},
        'BROADCAST':{'algorithm':'broadcast_hash','build_side':'right','reuse':'both existing shuffled inputs','additional_work':'broadcast right and build hash relation; probe with left'}},
      'accounting':'Already completed shuffles are sunk shared work. Neither choice avoids them. No measured candidate times are supplied.',
      'units':'Spark runtime size, serialized shuffle bytes and hash-table memory are distinct; hash memory and currently free heap are unknown.'}
    return canonical(full)
