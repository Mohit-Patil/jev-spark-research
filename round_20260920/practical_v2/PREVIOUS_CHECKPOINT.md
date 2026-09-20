# Latest research checkpoint — 20 September 2026

Repository: `Mohit-Patil/jev-spark-research` (private), branch `main`.
Workspace: `/Users/mohitpatil/JevResearch`. Native bridge/environment: `/Users/mohitpatil/jev-mac-native`.

## Read first

`round_20260920/aqe_observer/RESULTS_2026-09-20.md` is the full current report. `FINAL_AUDIT.json` in the same directory contains independently checked evidence and exact counts. `CANDIDATE_COVERAGE.json` records an important limit of a native current/proposed-only selector.

The previous onboarding checkpoint is superseded by this one. Its preserved audit remains `round_20260920/repository_audit_v1.json`. Earlier commits include 9f52e26 and 79870bb. The tested observer/candidate correction was committed and pushed as a46eaec; consult Git for subsequent results commits.

## Current evidence

This continuation executed 357 synthetic queries across six batches (including one superseded screening batch). All stored results matched the independent integer oracle. Twenty-five offline tests passed. The original pilot/held-out artifact hashes and frozen source remained unchanged.

The current/proposed observer was implemented and verified against Spark 4.0.1 source and installed bytecode (lines 365/366, release commit 29434ea766b0fc3c3bf6eaadb43a8f931133649e). The randomized observer check recorded 63 pairs without errors/unmatched calls. Six Jev requests were made during actual AQE comparisons, using real runtime statistics, with no applied model choice; all six abstained. HTTP median was 1.031 seconds, including fresh connection/network/server latency, not isolated inference.

Separate V3 fixed-arm trials executed 26 actual single-boundary AQE overrides with correct results. These were NOT Jev-directed. The first trial and fixed replication disagreed on a few-millisecond effect, so no robust runtime advantage or Jev speedup is established. Preserve state mismatches and compare only appropriately paired runs; do not use unpaired subset medians to claim gains.

Initial physical-plan deduplication is unsafe for hint variants with different future AQE behavior. Use the corrected four-candidate inventory in headroom_v3.py; do not silently alter the original frozen pilot. The retained-skew case had whole-query broadcast headroom but no broadcast candidate in any of its 27 observed native current/proposed pairs. The live case named live_skew filters out its hot key; it is not a surviving-skew runtime test.

## Budget and secrets

Latest audited durable count: 19/50 attempts, 31 remaining. Re-read `Budget().count()` at every resumption; other work can advance the shared ledger. Attempts 14-19 are this continuation's six live calls. Never reset the workspace ledger or print/read secrets through tools. Git ignores .env files and the ledger. Use the existing secret-scanning checkpoint helper for commits.

## Resume without contaminating results

Run `round_20260920/aqe_observer/final_audit.py` through the native bridge to verify saved results and tests without new Spark or Jev execution. It is safe to rerun and checks original preservation, result correctness, signatures, intervention scope and accounting.

Do not overwrite completed experiment directories. The next research branch is richer legal remaining-plan candidate generation plus better treatment of in-flight stage state and longer-running synthetic work, not repeated prompting against the same tested outcomes. Keep native AQE intact as comparator and freeze evaluation families before scoring.

The original eight-case pilot and the new runtime development trials are not a held-out production benchmark. The runtime state signature covers observed plan/stage fields, not a complete atomic process snapshot. No production-ready override hook, successful Jev selector, or generalized speedup is claimed.

Compiled classes/jars remain local and are not automatically tracked. The compiler invocations and source hashes are recorded, but a fresh clone must build in a new output directory and record new binary hashes; old result directories intentionally reject overwrite. Do not replace recorded original artifact hashes to pretend a rebuild is the same execution.

All jobs for this checkpoint completed within the native bridge's limits. No unattended or detached research job is scheduled. Keep scoped commits after tested code changes and completed evidence batches.
