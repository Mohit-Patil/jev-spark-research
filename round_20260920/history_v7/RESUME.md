# Latest Jev / Spark research checkpoint

Updated from audited evidence at 2026-09-20T18:37:52+00:00 (2026-09-21T00:07:52+05:30, Asia/Kolkata).
Private repository: Mohit-Patil/jev-spark-research, main. Workspace: ~/JevResearch. Native bridge: ~/jev-mac-native.

## Read first

round_20260920/history_v7/RESULTS.md and FINAL_AUDIT.json. This supersedes stale top-level 45/50 budget status without overwriting older experiment evidence.

## Latest executed iteration

202 query attempts: 200 correct completions and two failures. 69 new API attempts (68-136); latest verified cumulative count 136. The 40 prospective Jev workflows did not beat their paired native, tuned-native, or cheap history-selector runs. All 28 valid prospective responses chose BROADCAST; 12 request failures/timeouts used native fallback. Fifteen small known-answer controls passed.

Caching amortized a selective repeated-query episode: 2.738050 seconds including first miss and dataset-registration cost, versus native 4.494866 seconds, but local history was faster at 2.124640 seconds. The wide cache case failed and remains incomplete. A separate fresh-process diagnostic failed on its third broadcast with Spark's explicit insufficient-memory message. Do not report all queries as correct or present the failed cache batch as completed.

## Authorization and accounting

The user's 2026-09-20T18:08:42Z message removed the API/Git allowance restriction. New work still uses small per-job caps and durable cumulative accounting. The installed AuthorizedBudget module retains its 10000 engineering backstop; it was not binding for this iteration. Never use the legacy safe_jev.Budget writer beyond its original limit or reset the ledger. Use production_v4/authorized_budget.py and inspect AuthorizedBudget().status() before a live batch.

## Untested / blocked

history_v7/admission_guard.py was written but its native execution was blocked by the tool safety layer. It has NOT passed its proposed tests. Do not reroute a blocked operation or count it as executed. audit_all.py is an unexecuted earlier draft; use audit_final.py for the completed failure-aware audit.

## Research scope

This iteration selects before execution with AQE enabled; it adds no runtime AQE intervention. Prior genuine runtime work remains under production_v4 and aqe_observer. Native source versions, result directories, historical data, and frozen protocols are preserved. Memory/resource admission, wider-family generalization, confidence calibration, and distributed production performance remain unestablished.

No detached/unattended work is running. The next valuable work is resource-safe, historical family-separated prediction with strong local/native baselines, not repeated prompts on evaluated cases.
