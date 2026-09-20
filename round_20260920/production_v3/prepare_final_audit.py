"""Make an audit revision that preserves the initial failed test and verified retry.
No model calls, new Spark execution, secret reads or changes to original evidence.
"""
from pathlib import Path
import hashlib,json,subprocess,sys
HERE=Path(__file__).resolve().parent

def main():
    old=HERE/'audit_iteration.py';text=old.read_text()
    additions='''    # Preserve the partial work from the failed harness, not a fake completion marker.
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
'''
    replacements=[
      ("runtime=HERE/'runtime_safety_v1'/'COMPLETED.json'", "runtime=HERE/'runtime_safety_v2'/'COMPLETED.json'"),
      ("    auth=load(HERE/'BUDGET_AUTHORIZATION.json')",additions+"    auth=load(HERE/'BUDGET_AUTHORIZATION.json')"),
      ("'final_joins':sorted({op for r in xs for op in OPS if op in r['final_plan'].split('== Initial Plan ==')[0]}),",
       "'initial_joins':sorted({op for r in xs for op in OPS if op in r['initial_plan']}),\n              'final_joins':sorted({op for r in xs for op in OPS if op in r['final_plan'].split('== Initial Plan ==')[0]}),"),
      ("the historical adapter still has its old limit. No additional Jev requests were made by this iteration.",
       "this iteration did not change the historical adapter. The shared ledger now records {report['request_budget']['attempts_now']} attempts; other research work may advance it. No additional Jev requests were made by this iteration."),
      ("Runtime/loopback integration status: ", "Completed revised runtime/loopback integration status: "),
      (". The prepared integration script tests native fallback", ". The revised integration script tested native fallback"),
      ("Only completed checks in AUDIT.json count as executed.",
       "Sixteen loopback cases passed in the successful retry. The first harness attempt completed the same 16 local cases and one correct Spark query, then failed on Py4J conversion of the test counter; that failed job and partial results remain in the audit rather than being discarded."),
      ("The status above records whether a later retry completed.",
       "The later retry completed successfully after a JVM-only test-counter setter fixed the Python/JVM interoperability bug."),
      ("More runtime alone does not create optimization headroom.",
       "More runtime alone does not create optimization headroom. The selective wide-data case, however, showed an early-broadcast gain in every measured pair, so unconditional NATIVE and unconditional BROADCAST are both insufficient to describe these development results.")]
    for a,b in replacements:
        assert text.count(a)==1, 'audit_revision_anchor_not_unique'
        text=text.replace(a,b)
    # The changed sentence lives in a single-quoted f-string: use double-quoted key expressions.
    text=text.replace("{report['request_budget']['attempts_now']}",'{report["request_budget"]["attempts_now"]}')
    revised=HERE/'audit_iteration_v2.py'
    compile(text,str(revised),'exec')
    with revised.open('x') as f:f.write(text)
    p=subprocess.run([sys.executable,str(revised),'--label','audit_v1'],cwd=HERE,capture_output=True,text=True,timeout=120)
    (HERE/'final_audit_execution.json').write_text(json.dumps({'returncode':p.returncode,
        'stdout':p.stdout,'stderr':p.stderr,'source_sha256':hashlib.sha256(revised.read_bytes()).hexdigest()},indent=2))
    print(p.stdout,flush=True)
    if p.returncode:print(p.stderr[-12000:],flush=True)
    raise SystemExit(p.returncode)
if __name__=='__main__':main()
