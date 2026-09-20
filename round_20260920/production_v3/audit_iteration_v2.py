"""Read-only experiment audit plus report creation. No new Spark queries or HTTP.
Audit output has a new label; raw experiments and original source are not modified.
"""
from pathlib import Path
import argparse,fcntl,hashlib,json,os,statistics,sys,time
HERE=Path(__file__).resolve().parent
ROUND=HERE.parent
WS=ROUND.parent
sys.path.insert(0,str(ROUND))
from safe_jev import Budget,LEDGER,LIMIT
from parquet_corpus import oracle,ARMS,OPS
BATCHES=['parquet_smoke_v1','parquet_narrow_v1','parquet_wide_small_v1','parquet_wide_large_v1','parquet_wide_selective_v1']

def load(p):return json.loads(p.read_text())
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def median(xs):return statistics.median(xs) if xs else None

def main():
    p=argparse.ArgumentParser();p.add_argument('--label',default='audit_v1');a=p.parse_args()
    assert a.label.replace('_','').isalnum()
    out=HERE/a.label;out.mkdir(exist_ok=False)
    source_hash=digest(HERE/'parquet_corpus.py');before=Budget().count()
    report={'kind':'audit_of_this_iteration_only','created_unix':time.time(),'new_spark_queries':0,'new_api_requests':0,
      'batches':[],'pending_batches':[],'queries_verified':0,'formal_measured_queries':0,'warmup_queries':0,
      'diagnostic_queries':0,'worst_paired_alternative_regression':None,'source_hashes':{},'failures':[]}
    for name in BATCHES:
        root=HERE/name
        if not (root/'COMPLETED.json').exists():
            report['pending_batches'].append(name);continue
        manifest=load(root/'manifest.json');completion=load(root/'COMPLETED.json')
        rows=[json.loads(x) for x in (root/'raw.jsonl').read_text().splitlines()]
        assert completion['status']=='succeeded' and len(rows)==completion['queries']
        assert manifest['source_sha256']==source_hash
        want=oracle(manifest['case'])
        assert all(r['status']=='succeeded' and r['correct'] and r['result']==want for r in rows)
        assert len(rows)==len(ARMS)*(manifest['warmups']+manifest['reps'])
        config=load(root/'config.json');assert config['spark.sql.adaptive.enabled']=='true'
        report['queries_verified']+=len(rows)
        measured=[r for r in rows if r['phase']=='measured']
        diagnostic=manifest['case_name']=='smoke'
        if diagnostic:report['diagnostic_queries']+=len(rows)
        else:
            report['formal_measured_queries']+=len(measured)
            report['warmup_queries']+=len(rows)-len(measured)
        native={r['rep']:r['to_result_s'] for r in measured if r['arm']=='NATIVE'}
        b={'batch':name,'case':manifest['case'],'case_name':manifest['case_name'],
          'queries':len(rows),'all_correct':True,'diagnostic_only':diagnostic,
          'config':config,'arms':{},'raw_sha256':digest(root/'raw.jsonl'),
          'manifest_sha256':digest(root/'manifest.json'),'data':load(root/'dataset.json')}
        for arm in ARMS:
            xs=[r for r in measured if r['arm']==arm]
            times=[r['to_result_s'] for r in xs];broadcast_sizes=[];spill=[]
            for r in xs:
                for node in r.get('sql_metrics',[]):
                    ms=node['metrics']
                    if node['node']=='BroadcastExchange' and 'dataSize' in ms:
                        broadcast_sizes.append(ms['dataSize']['value'])
                    if 'spillSize' in ms:spill.append(ms['spillSize']['value'])
                ratio=r['to_result_s']/native[r['rep']]
                if not diagnostic and arm!='NATIVE':
                    old=report['worst_paired_alternative_regression']
                    if old is None or ratio>old['ratio']:
                        report['worst_paired_alternative_regression']={'case':manifest['case_name'],'arm':arm,
                          'rep':r['rep'],'ratio':ratio,'extra_s':r['to_result_s']-native[r['rep']]}
            b['arms'][arm]={'n':len(xs),'median_s':median(times),'min_s':min(times),'max_s':max(times),
              'faster_than_native_pairs':sum(r['to_result_s']<native[r['rep']] for r in xs),
              'median_paired_saving_s':median([native[r['rep']]-r['to_result_s'] for r in xs]),
              'worst_ratio_to_native':max(r['to_result_s']/native[r['rep']] for r in xs),
              'initial_joins':sorted({op for r in xs for op in OPS if op in r['initial_plan']}),
              'final_joins':sorted({op for r in xs for op in OPS if op in r['final_plan'].split('== Initial Plan ==')[0]}),
              'broadcast_exchange_reported_data_size_bytes':sorted(set(broadcast_sizes)) or None,
              'captured_operator_spill_size_values':sorted(set(spill)) if spill else None}
        saved=load(root/'summary.json')
        for arm in ARMS:assert abs(b['arms'][arm]['median_s']-saved['arms'][arm]['median_s'])<1e-12
        report['batches'].append(b)
    safety=HERE/'observer_safety_v1'/'COMPLETED.json'
    report['compiled_guard_tests']=load(safety) if safety.exists() else {'status':'not_run'}
    runtime=HERE/'runtime_safety_v2'/'COMPLETED.json'
    report['runtime_safety']=load(runtime) if runtime.exists() else {'status':'not_run_or_incomplete'}
    if runtime.exists():
        assert report['runtime_safety']['status']=='succeeded'
        rs=[json.loads(x) for x in (runtime.parent/'spark_raw.jsonl').read_text().splitlines()]
        assert all(r['correct'] and r['status']=='succeeded' for r in rs)
        report['queries_verified']+=len(rs);report['diagnostic_queries']+=len(rs)
    # Preserve the partial work from the failed harness, not a fake completion marker.
    from baseline import expected as base_expected
    fixture=dict(name='guarded_runtime_smoke',n=1200000,m=2000000,keep=100,skew=False,seed=19)
    want_runtime=base_expected(fixture)
    partial=HERE/'runtime_safety_v1'
    partial_rows=[json.loads(x) for x in (partial/'spark_raw.jsonl').read_text().splitlines()]
    assert len(partial_rows)==1 and all(r['status']=='succeeded' and r['result']==want_runtime for r in partial_rows)
    assert not (partial/'COMPLETED.json').exists()
    report['queries_verified']+=len(partial_rows);report['diagnostic_queries']+=len(partial_rows)
    report['failures'].append({'job_id':'1e2283b5cfc6483fb4477b1e49578537',
        'script':'runtime_safety_checks.py','status':'failed','returncode':1,
        'error':'AttributeError: Py4J returned int, so obj.pairs().set(1000) failed',
        'completed_correct_queries_before_failure':len(partial_rows),
        'raw_sha256':digest(partial/'spark_raw.jsonl'),
        'repair':'JVM-only observer test-counter setter in runtime_fault_helper; retry preserved separately in runtime_safety_v2'})
    report['loopback_fault_runs']=[]
    for d in ('runtime_safety_v1','runtime_safety_v2'):
        faults=load(HERE/d/'loopback_faults.json')
        assert len(faults)==16
        assert len({(r['native'],r['mode']) for r in faults})==16
        for r in faults:
            assert r['effective']==('PROPOSED' if r['mode']=='success' else r['native'])
            if r['mode']=='trickle':assert r.get('error_type')=='TimeoutError' and .12<=r['wall_s']<.6
        report['loopback_fault_runs'].append({'directory':d,'checked':16,
            'trickle_wall_s':[r['wall_s'] for r in faults if r['mode']=='trickle'],
            'sha256':digest(HERE/d/'loopback_faults.json')})
    for r in rs:assert r['result']==want_runtime
    auth=load(HERE/'BUDGET_AUTHORIZATION.json')
    fd=os.open(LEDGER,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
    with os.fdopen(fd,'rb') as f:
        fcntl.flock(f.fileno(),fcntl.LOCK_SH)
        prefix=f.read(auth['prior_ledger_bytes'])
    assert hashlib.sha256(prefix).hexdigest()==auth['prior_ledger_sha256']
    report['request_budget']={'authorized_total':auth['authorized_max_attempts'],
      'recorded_authorization_utc':auth['authorized_at_user_message_utc'],
      'attempts_at_authorization':auth['attempts_at_authorization'],'attempts_now':Budget().count(),
      'prior_history_prefix_verified':True,'legacy_adapter_limit':LIMIT,
      'new_adapter_present':(HERE/'cumulative_budget.py').exists(),
      'enforcement_update_status':'Attempted new accounting module write was blocked by tool safety; no bypass or legacy-source modification attempted.',
      'new_jev_attempts_by_this_iteration':0}
    preflight=load(HERE/'preflight.json')
    report['preserved_prior_sources']={name:digest(WS/name)==old for name,old in preflight['source_hashes'].items()}
    for f in HERE.glob('*.py'):report['source_hashes'][str(f.relative_to(WS))]=digest(f)
    report['api_before_audit']=before;report['api_after_audit']=Budget().count();report['status']='succeeded'
    report['measurement_limitations']=['Three repetitions per arm in development screens; no independent family holdout scored.',
      'Warmed local Parquet/JVM/OS-cache setting, not distributed or cold-storage validation.',
      'BroadcastExchange dataSize is not whole-process peak memory.',
      'No Jev choices or Jev-directed runtime interventions in this iteration.',
      'The new guard addresses V3 truncation/audit persistence only, not complete executed-prefix equivalence or every older extension.']
    (out/'AUDIT.json').write_text(json.dumps(report,indent=2))
    lines=['# File-backed plan-choice and observer-safety iteration','',
      '20 September 2026. Native Mac mini, Spark 4.0.1. This report covers only production_v3 work.','',
      '## Authorization and execution status','',
      f'The user authorized 10,000 cumulative Jev attempts. {auth["attempts_at_authorization"]} prior attempts were preserved without changing their ledger bytes. The attempted enforcement-module write was blocked by the tool safety check; this iteration did not change the historical adapter. The shared ledger now records {report["request_budget"]["attempts_now"]} attempts; other research work may advance it. No additional Jev requests were made by this iteration.','',
      f'Audit verified {report["queries_verified"]} completed Spark queries: {report["formal_measured_queries"]} comparative measurements, {report["warmup_queries"]} warmups and {report["diagnostic_queries"]} diagnostic queries. Every checked result matched the Python integer oracle.','',
      '## Hypothesis and method','',
      'Test whether plan selection still matters after moving beyond narrow generated inputs: materialize real Parquet files, retain a wide string across LEFT joins, include null keys, and compare default native, tuned native, broadcast, merge and shuffled-hash policies. A cross-input checksum prevents the payload from being projected away before the join.','',
      'All arms keep AQE on. File-backed screens use local[2], a requested 2 GiB heap, 32 shuffle partitions, one warmup and three measured randomized rounds per arm. SQL construction, initial-plan capture and collection are timed. Input generation, correctness-oracle work, and final-plan/metrics logging are excluded. DataFrames are not cached, but JVM and OS caches are warm.','',
      '## Measured candidate execution medians (seconds)','',
      '| Workload | Native | Tuned 64 MiB | Broadcast | Merge | Shuffle hash |',
      '|---|---:|---:|---:|---:|---:|']
    for b in report['batches']:
        if not b['diagnostic_only']:
            lines.append('| '+b['case_name']+' | '+' | '.join(f'{b["arms"][x]["median_s"]:.4f}' for x in ARMS)+' |')
    lines+=['','The wide-small-probe case is a counterexample to unconditional broadcasting: native sort-merge beat forced broadcast in every measured round. On the wide-large-probe case, broadcast essentially tied native despite a roughly 3.4-second query duration. More runtime alone does not create optimization headroom. The selective wide-data case, however, showed an early-broadcast gain in every measured pair, so unconditional NATIVE and unconditional BROADCAST are both insufficient to describe these development results. Small differences between arms with the same final operator are not treated as proven effects of configuration.','',
      '## Safety implementation and validation','',
      f'The separate guarded observer derivative compiled on the Mac and passed {report["compiled_guard_tests"].get("jvm_tests",{}).get("checks_passed",0)} JVM checks. It rejects truncated or incomplete plan descriptors before state matching and returns stock costs when its audit record cannot be persisted. The original V3 source is preserved.','',
      'Completed revised runtime/loopback integration status: '+report['runtime_safety']['status']+'. The revised integration script tested native fallback for both CURRENT and PROPOSED, malformed and stale replies, a slow-drip response with a 150 ms overall async deadline, and two Spark pass-through/audit-capacity queries. Sixteen loopback cases passed in the successful retry. The first harness attempt completed the same 16 local cases and one correct Spark query, then failed on Py4J conversion of the test counter; that failed job and partial results remain in the audit rather than being discarded.','',
      'These tests do not prove a production-safe optimizer, complete executed-prefix matching, distributed memory safety, or an operating-system hard-real-time deadline. The deadline test path is not yet a deployed replacement for the live Jev adapter.','',
      '## Pending work and reproduction','',
      'Pending corpus batches: '+(', '.join(report['pending_batches']) or 'none')+'.',
      'Use a new output label when repeating an experiment; never overwrite completed evidence. Keep query-family holdouts separate. The next useful model comparison must beat strong inexpensive policies on workloads where the best strategy varies, using only statistics available at the actual decision time.','',
      'The Mac worker returned a busy response to attempted safety-integration launches; another tracked research job was not interrupted. The later retry completed successfully after a JVM-only test-counter setter fixed the Python/JVM interoperability bug. No unattended continuation was scheduled by this iteration.','',
      'Raw per-query timings, physical plans, SQL metrics, manifests and completion markers are saved in each parquet_* directory. AUDIT.json contains recomputed medians, regressions, reported broadcast sizes, source hashes and request-accounting status. Git checkpoint results are recorded separately in last_git_checkpoint.json after a verified commit/push.','',
      '## Public references','',
      '- Apache Spark 4.0.1 performance tuning: https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html',
      '- HTTPX timeout semantics: https://www.python-httpx.org/advanced/timeouts/','']
    (out/'RESULTS.md').write_text('\n'.join(lines))
    brief={k:report[k] for k in ('status','queries_verified','formal_measured_queries','warmup_queries','diagnostic_queries','pending_batches','worst_paired_alternative_regression','request_budget','preserved_prior_sources')}
    brief['guard_jvm_checks']=report['compiled_guard_tests'].get('jvm_tests',{}).get('checks_passed')
    brief['runtime_safety_status']=report['runtime_safety']['status']
    brief['broadcast_sizes']={b['case_name']:b['arms']['BROADCAST']['broadcast_exchange_reported_data_size_bytes'] for b in report['batches']}
    print(json.dumps(brief,indent=2))
if __name__=='__main__':main()
