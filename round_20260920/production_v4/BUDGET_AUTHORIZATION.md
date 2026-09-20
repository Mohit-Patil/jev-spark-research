# Cumulative API budget authorization

At 2026-09-20T17:21:22Z the user explicitly increased the cumulative TypeSafe Jev request allowance from 50 to 10,000 attempts. This is not 10,000 additional attempts. Existing attempts, failures, and interrupted reservations remain charged.

`authorized_budget.py` appends under the same exclusive file lock to the original workspace ledger. It validates a private authorization record and a cryptographic digest of every pre-existing ledger byte before counting/reserving. It fails closed if the ledger disappears, truncates, changes its historical prefix, has broken sequence numbers, exceeds request size bounds, or reaches the cap. No implicit resets or retries are performed. A separate per-batch cap keeps experiments bounded.

The original frozen `safe_jev.py` remains unchanged for reproducibility. Its legacy writer still refuses requests at 50; new runners must explicitly pass `AuthorizedBudget` or `BatchBudget` to `BoundedClient`. Reading the old ledger is compatible because its original header and all attempt records are preserved. The historical header's 50 describes the first authorization, not the active amended cap.

Private accounting/authorization files are kept outside the tracked artifact allowlist. No API key was needed for this update. The 600-second native bridge job limit and all other safety constraints are unchanged.
