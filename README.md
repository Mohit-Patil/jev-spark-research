# Jev / Spark native research

Research hypothesis: can TypeSafe Jev improve a bounded choice among legal Spark query plans, eventually using information observed at genuine AQE decision points? A negative result is acceptable.

## Environment and scope

Native macOS ARM64, CPU-only, Spark 4.0.1, Java 17, Python 3.11. Experiments start with local[2], a 1 GiB driver heap, and synthetic data. The original lab ZIP was absent in the inspected workspace; the round_20260920 pilot is an independent implementation. No Docker, EC2, or company data.

The existing baseline/live pilot chooses whole-query strategies **before** execution with AQE enabled. It is not an AQE intervention. Live-pilot totals formed by combining separately measured candidate runtimes and advisor costs are **offline estimates**, not measured end-to-end speedups.

## Files

`round_20260920/baseline.py` captures initial/final plans, randomized warmups/repetitions, and exact aggregate checks. `safe_jev.py` bounds requests and validates the pinned model/choice output. `live_pilot.py` saves decisions before candidate execution and freezes a policy before holdout. `repository_audit_v1.json` checks the saved records and reports API usage.

`round_20260920/repo_checkpoint.py --message "description" --publish` scans approved research files for secrets, commits changes, and pushes only to the verified private repository using existing GitHub CLI authentication. Run it through the connected native research tool or the existing project Python environment. Commit at completed, inspected milestones; no detached automation is installed.

## Secret and request safety

Keep the locally configured `.env` out of Git. Do not paste keys in issues, shell arguments, logs, prompts, or reports. `.gitignore` excludes environment files and the workspace-wide durable `.jev_initial_round_attempts.jsonl` ledger. The initial research round has a cumulative limit of 50 request attempts, including failed/interrupted attempts. **Never delete or reset the ledger to obtain a fresh budget.** A clone is not permission to restart that allowance.

## Reproduction and interpretation

Inspect manifests, raw JSONL, completed markers, and final plans together. Preserve failures. Holdout estimates must not be treated as direct policy timing. Current workloads are small synthetic instances from one query template, not representative production evidence. See CHECKPOINT.md and RESEARCH_LOG.md for the resumption boundary.
