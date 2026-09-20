# Jev / Spark research context

The user wants to test whether TypeSafe Jev can make useful bounded choices
between Spark execution strategies, particularly when AQE has measured runtime
statistics. Treat the idea as a hypothesis; a negative result is acceptable.

This worker is only the execution connection. No live Jev benchmark has been
established by this kit. Start with the health check and included Spark smoke
test. The earlier research lab ZIP is a separate input and must be inspected
before use. Do not assume earlier text about tests establishes an actual Spark
run.

Use synthetic data. Keep native Spark with AQE as the baseline. Distinguish
pre-execution candidate selection with AQE on from actual interventions at AQE
replanning boundaries. Include planning, feature preparation and Jev latency in
total cost. Preserve actual initial/final plans and raw timings. Verify equivalent
results. Count severe regressions and failures, not merely average improvement.

Review and approve a bounded experiment and API request limit before introducing
a disposable Jev key. The execution bridge itself does not enforce request
budgets. Do not disclose any keys in outputs, logs or source code.
