# Repository onboarding checkpoint — 20 September 2026

The private repository is `Mohit-Patil/jev-spark-research`, branch `main`, working tree `~/JevResearch`. Native bridge: `~/jev-mac-native`. Both directories were verified on disk.

Initial checkpoint commit: `9f52e26d48cae839b444829ef4f6d45e81f693a0`. Its push was verified successful by the native job. Existing work was preserved before adding this audit.

The repository audit reruns only offline tests and validates previously saved baseline/validation/holdout files. It does not rerun Spark, invoke Jev, or claim ownership of prior executions. Refer to `round_20260920/repository_audit_v1.json` for counts, hashes, and uncertainty.

Resume by reading the current request ledger via `Budget().count()` and inspecting the latest working tree/commit first. Do not overwrite completed result directories or re-tune on the tested holdout. The existing freeze guards only live_pilot.py; future freezes should include imported code/config hashes. Avoid modifying the original frozen experiment.

The small-query pilot and genuine AQE runtime observation/intervention are separate milestones. Verify any later observation-hook artifacts and their job completion independently before claiming that milestone. A remote selector on the critical path needs measured end-to-end evidence, not merely an offline estimate.
