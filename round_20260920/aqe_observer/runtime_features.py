"""Explicit evidence whitelist for runtime shadow advice; excludes native labels and outcomes."""
import copy

STAGE_FIELDS=('stage_id','node','materialized','size_in_bytes','row_count','is_runtime',
 'shuffle_serialized_partition_bytes','partition_count','nonempty_partitions','partition_bytes_total',
 'partition_bytes_max','partition_bytes_p50','partition_bytes_p95')
CRITERIA={'CURRENT':'Continue Spark current remaining physical plan, retaining work already completed.',
          'PROPOSED':'Use Spark proposed remaining physical plan; account for any extra exchanges or materialization it needs.',
          'ABSTAIN':'Insufficient evidence to improve on native Spark; defer to the native decision.'}
INSTRUCTIONS=('Choose the supplied remaining plan expected to finish sooner on this CPU-only Spark local[2] machine, or ABSTAIN. '
 'This is shadow advice at a real AQE boundary. No intervention is applied. Use only this snapshot; '
 'do not treat whole-query hinted timings as counterfactual runtime outcomes. Completed work is sunk cost, '
 'not an avoidable future saving. Spark-reported size statistics, shuffle partition byte reports and hash-table '
 'memory are different measurements. Unknown means unknown, not zero. Advisor overhead is measured separately. '
 'Do not assume PROPOSED is faster merely because Spark proposed it.')

def project(pair,view):
    if view not in ('PLANS','STATS'):raise ValueError('unknown_view')
    if pair.get('kind')!='native_aqe_cost_boundary_pair_v2' or pair.get('spark_version')!='4.0.1':raise ValueError('wrong_snapshot')
    for key,line in [('current',365),('proposed',366)]:
        if pair[key]['source_line']!=line or pair[key].get('plan_truncated',True):raise ValueError('unverified_or_truncated_snapshot')
    if pair.get('intervention') or not pair.get('cost_returned_unchanged'):raise ValueError('not_shadow')
    state={'engine':'Apache Spark 4.0.1','mode':'shadow advice inside a current-versus-proposed AQE cost comparison',
      'master':'local[2]','cpu_only':True,
      'candidates':{'CURRENT':pair['current']['remaining_physical_plan'],'PROPOSED':pair['proposed']['remaining_physical_plan']},
      'completed_stage_ids':sorted({s['stage_id'] for k in ('current','proposed') for s in pair[k]['stages'] if s['materialized']}),
      'resources':{k:pair.get('resources',{}).get(k) for k in ('jvm_heap_max_bytes','jvm_heap_committed_bytes','jvm_heap_free_bytes')},
      'unmeasured':{'alternative_remaining_runtimes':None,'extra_materialization_latency':None,'hash_table_peak_memory_bytes':None},
      'caveat':'physical plans themselves can encode runtime adaptations; PLANS is not devoid of all runtime information'}
    if view=='STATS':
        state['runtime_stage_observations']={k:[{f:copy.deepcopy(s.get(f)) for f in STAGE_FIELDS} for s in pair[k]['stages']] for k in ('current','proposed')}
        state['measurement_semantics']='Spark runtime statistics and reported map-output partition bytes, captured at this boundary; not final query statistics, not proof of in-memory hash-table size. Visible stage leaves are not a complete executed-work history.'
    return state
