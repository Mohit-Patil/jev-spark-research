# Latest practical research checkpoint — 20 September 2026

Private repository: `Mohit-Patil/jev-spark-research`, branch `main`.
Workspace: `~/JevResearch`; native bridge/environment: `~/jev-mac-native`.

Read `round_20260920/practical_v2/RESULTS_2026-09-20.md` and `FINAL_AUDIT.json` first.

This continuation completed 218 new synthetic Spark queries (135 measured, 80 warmups, 3 runtime diagnostics), all exact results correct. Thirty-four new offline tests passed. The prior 357 queries and 25 tests were separately audited, not re-executed as new experiments.

Jev with a pooled client achieved some directly measured pre-execution gains over default Spark, but a stronger bounded broadcast rule beat Jev in all six paired trials. A fixed 64 MiB native broadcast threshold beat default native in nine of nine paired trials in the tested numeric-schema domain. This is not a generally safe production threshold or evidence of incremental model value.

The scaled actual AQE check captured ten pairs across three queries with stock costs unchanged. Two workloads had no broadcast candidate at any comparison; the one live changed-pair request abstained. No new Jev-directed AQE override or stable remaining-time benefit is established.

Final audited ledger: 45/50 used, 5 remaining. Re-read the durable ledger before any subsequent call; never reset it. This continuation used requests 20–45. No additional paid APIs, dependencies, global Spark configuration changes or cloud resources were introduced.

Resume verification with `round_20260920/practical_v2/audit_results.py`. It reruns offline tests and audits saved results, not live Spark or Jev. Preserve all frozen sources and result directories. The next corpus must challenge tuned native and cheap policies and include varying optimal strategies, realistic feature acquisition and stronger runtime-state coverage. Read the report's proposed evidence gates before new live calls.

The prior checkpoint text is preserved at `round_20260920/practical_v2/PREVIOUS_CHECKPOINT.md`; earlier AQE evidence remains under `round_20260920/aqe_observer/`. Milestone f150ecf was pushed; the final report commit is logged by the later checkpoint job. No unattended continuation was scheduled here.
