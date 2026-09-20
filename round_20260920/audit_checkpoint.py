"""Verify saved experiment records and create a resumable Git checkpoint.
No Spark execution and no network requests. Does not read the API key.
"""
from pathlib import Path
import hashlib, json, math, os, statistics, subprocess, sys, time
ROOT=Path(__file__).resolve().parent
WS=ROOT.parent
from safe_jev import Budget

def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def main():
    report={'audit_unix':time.time(),'origin':'audit of existing saved runs, not new benchmark executions','batches':[]}
    used_before=Budget().count()
    test=subprocess.run([sys.executable,str(ROOT/'offline_checks.py')],cwd=ROOT,capture_output=True,text=True,timeout=60,env={**os.environ,'PYTHONDONTWRITEBYTECODE':'1'})
    report['offline_tests']=json.loads((ROOT/'offline_tests.json').read_text())
    if test.returncode or not report['offline_tests']['successful']:raise RuntimeError('offline_tests_failed')
    for label in ('baseline_v1','live_validation','live_holdout'):
        directory=ROOT/label
        if not (directory/'COMPLETED.json').is_file():
            report['batches'].append({'batch':label,'status':'not_completed'});continue
        paths=[directory/x for x in ('COMPLETED.json','raw.jsonl','summary.json','manifest.json')]
        hashes={str(p.relative_to(WS)):sha(p) for p in paths}
        complete=json.loads(paths[0].read_text());raw=[json.loads(x) for x in paths[1].read_text().splitlines()];summaries=json.loads(paths[2].read_text());manifest=json.loads(paths[3].read_text())
        if complete['status']!='succeeded' or not complete['all_correct']:raise RuntimeError('completion_failure')
        if any(r['status']!='succeeded' or not r['correct'] for r in raw):raise RuntimeError('raw_failure')
        if any(not math.isfinite(r['to_result_s']) or r['to_result_s']<=0 for r in raw):raise RuntimeError('invalid_timing')
        for case in set(r['case'] for r in raw):
            results={json.dumps(r['result'],sort_keys=True) for r in raw if r['case']==case}
            if len(results)!=1:raise RuntimeError('result_disagreement')
        case_report=[]
        for s in summaries:
            case=s['case'];rs=[r for r in raw if r['case']==case]
            if len(rs)!=21:raise RuntimeError('unexpected_run_count')
            med={k:statistics.median(r['to_result_s'] for r in rs if r['strategy']==k and r['phase']=='measured') for k in {r['strategy'] for r in rs}}
            for k,v in med.items():
                recorded=s['strategies'][k]['median_s'] if label=='baseline_v1' else s['candidate_median_s'][k]
                if abs(recorded-v)>1e-10:raise RuntimeError('summary_mismatch')
                if len([r for r in rs if r['strategy']==k and r['phase']=='measured'])!=5:raise RuntimeError('repetition_count')
            case_report.append({'case':case,'measured_median_s':med,'lowest_median_candidate':min(med,key=med.get),'native_is_lowest_median':med['NATIVE']==min(med.values()),'final_aqe_plan_flag_all_runs':all('isFinalPlan=true' in r['final_plan'] for r in rs)})
        prediction_order=None
        if label.startswith('live_'):
            decisions=[json.loads(x) for x in (directory/'decisions.jsonl').read_text().splitlines()]
            prediction_order=all(d['record']['completed_unix']<=min(r['started_unix'] for r in raw if r['case']==d['case']) for d in decisions)
            if not prediction_order:raise RuntimeError('prediction_order_failure')
        if any(sha(WS/name)!=digest for name,digest in hashes.items()):raise RuntimeError('files_changed_during_audit')
        report['batches'].append({'batch':label,'status':'verified_saved_records','runs_including_warmups':len(raw),'measured_runs':sum(r['phase']=='measured' for r in raw),'all_results_equivalent':True,'predictions_before_first_execution':prediction_order,'cases':case_report,'sha256':hashes})
    if (ROOT/'frozen_policy.json').exists():
        freeze=json.loads((ROOT/'frozen_policy.json').read_text())
        report['frozen_policy']=freeze['policy']
        report['frozen_main_source_matches']=sha(ROOT/'live_pilot.py')==freeze['source_sha256']
        report['freeze_scope_caveat']='Original freeze hashes live_pilot.py only, not imported baseline.py and safe_jev.py; preserve their Git versions too.'
    calls=[json.loads(x) for x in (ROOT/'jev_calls.jsonl').read_text().splitlines()]
    report['api_attempts_before_audit']=used_before;report['api_attempts_after_audit']=Budget().count();report['api_limit']=50
    report['saved_call_records']=len(calls)
    report['saved_successful_calls']=sum(r['status']=='succeeded' for r in calls)
    report['saved_latency_s']={'min':min(r['latency_s'] for r in calls),'median':statistics.median(r['latency_s'] for r in calls),'max':max(r['latency_s'] for r in calls)}
    report['saved_usage']={k:sum(r.get('usage',{}).get(k,0) for r in calls) for k in ('input_tokens','output_tokens')}
    report['new_jev_attempts_by_audit']=0
    out=ROOT/'repository_audit_v1.json'
    if out.exists():raise RuntimeError('audit_destination_exists')
    out.write_text(json.dumps(report,indent=2))
    readme=WS/'README.md'
    if not readme.exists():
        readme.write_text('''# Jev / Spark native research\n\nResearch hypothesis: can TypeSafe Jev improve a bounded choice among legal Spark query plans, eventually using information observed at genuine AQE decision points? A negative result is acceptable.\n\n## Environment and scope\n\nNative macOS ARM64, CPU-only, Spark 4.0.1, Java 17, Python 3.11. Experiments start with local[2], a 1 GiB driver heap, and synthetic data. The original lab ZIP was absent in the inspected workspace; the round_20260920 pilot is an independent implementation. No Docker, EC2, or company data.\n\nThe existing baseline/live pilot chooses whole-query strategies **before** execution with AQE enabled. It is not an AQE intervention. Live-pilot totals formed by combining separately measured candidate runtimes and advisor costs are **offline estimates**, not measured end-to-end speedups.\n\n## Files\n\n`round_20260920/baseline.py` captures initial/final plans, randomized warmups/repetitions, and exact aggregate checks. `safe_jev.py` bounds requests and validates the pinned model/choice output. `live_pilot.py` saves decisions before candidate execution and freezes a policy before holdout. `repository_audit_v1.json` checks the saved records and reports API usage.\n\n`round_20260920/repo_checkpoint.py --message "description" --publish` scans approved research files for secrets, commits changes, and pushes only to the verified private repository using existing GitHub CLI authentication. Run it through the connected native research tool or the existing project Python environment. Commit at completed, inspected milestones; no detached automation is installed.\n\n## Secret and request safety\n\nKeep the locally configured `.env` out of Git. Do not paste keys in issues, shell arguments, logs, prompts, or reports. `.gitignore` excludes environment files and the workspace-wide durable `.jev_initial_round_attempts.jsonl` ledger. The initial research round has a cumulative limit of 50 request attempts, including failed/interrupted attempts. **Never delete or reset the ledger to obtain a fresh budget.** A clone is not permission to restart that allowance.\n\n## Reproduction and interpretation\n\nInspect manifests, raw JSONL, completed markers, and final plans together. Preserve failures. Holdout estimates must not be treated as direct policy timing. Current workloads are small synthetic instances from one query template, not representative production evidence. See CHECKPOINT.md and RESEARCH_LOG.md for the resumption boundary.\n''')
    checkpoint=WS/'CHECKPOINT.md'
    if not checkpoint.exists():
        checkpoint.write_text('''# Repository onboarding checkpoint — 20 September 2026\n\nThe private repository is `Mohit-Patil/jev-spark-research`, branch `main`, working tree `~/JevResearch`. Native bridge: `~/jev-mac-native`. Both directories were verified on disk.\n\nInitial checkpoint commit: `9f52e26d48cae839b444829ef4f6d45e81f693a0`. Its push was verified successful by the native job. Existing work was preserved before adding this audit.\n\nThe repository audit reruns only offline tests and validates previously saved baseline/validation/holdout files. It does not rerun Spark, invoke Jev, or claim ownership of prior executions. Refer to `round_20260920/repository_audit_v1.json` for counts, hashes, and uncertainty.\n\nResume by reading the current request ledger via `Budget().count()` and inspecting the latest working tree/commit first. Do not overwrite completed result directories or re-tune on the tested holdout. The existing freeze guards only live_pilot.py; future freezes should include imported code/config hashes. Avoid modifying the original frozen experiment.\n\nThe small-query pilot and genuine AQE runtime observation/intervention are separate milestones. Verify any later observation-hook artifacts and their job completion independently before claiming that milestone. A remote selector on the critical path needs measured end-to-end evidence, not merely an offline estimate.\n''')
    log=WS/'RESEARCH_LOG.md';old=log.read_text() if log.exists() else '# Research log\n'
    log.write_text(old+'\n## GitHub onboarding and saved-record audit\n\nHypothesis: the current research can be checkpointed without disclosing secrets or changing the cumulative API allowance. Experiment: initialize private GitHub repository, inspect native Git/gh authentication, scan approved files, push initial commit, rerun offline tests, recompute candidate medians and verify prediction ordering. Measurements: see round_20260920/repository_audit_v1.json. Interpretation: saved-record integrity is separate from a newly executed benchmark, production generalization, and genuine AQE intervention. Next change: make reviewed commits after completed experiment/code milestones; retain the untouched original runs and cumulative ledger.\n')
    print(json.dumps(report,indent=2))

if __name__=='__main__':
    try:main()
    except Exception as e:
        print(json.dumps({'status':'failed','error_type':type(e).__name__,'reason':str(e) if type(e) is RuntimeError else 'details_suppressed'}));raise SystemExit(1)
