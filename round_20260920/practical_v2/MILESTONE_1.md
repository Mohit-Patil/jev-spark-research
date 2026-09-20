# Practical research continuation: verified checkpoint 1

20 September 2026. Native Mac mini only; no dependency installation.

## Inherited work verified, not re-attributed

`aqe_observer/final_audit.py` completed successfully in job `6b9a7a90686848c7a70a4966ae48b54b`: 25 tests passed and all 357 saved queries matched the independent integer oracle. The older candidate-inventory repair and 26 fixed-arm runtime interventions were already present when this continuation inspected the workspace. They are not new executions initiated by this continuation. Six earlier live runtime recommendations abstained; no beneficial Jev-directed AQE override was established.

## Newly executed

The ten offline workload tests and twelve mock client tests passed. The client tests cover failed/late responses, model mismatch, request and response size bounds, no retry on error, secret-safe traces, and native-PROPOSED fallback rather than automatic CURRENT.

The new four-arm benchmark preserves every join-hint policy even when initial plans collide. It uses a requested 2 GiB heap and 32 shuffle partitions, identical across arms, with AQE and automatic broadcasting enabled. It adds LEFT joins so predicate propagation cannot discard most fact rows as in the small INNER workloads. Timing includes SQL construction, initial-plan capture and collection; candidate enumeration and final-plan logging are excluded from this candidate-only screen.

- `recheck_inner_10_v1`: 20 correct runs, 12 measured. NATIVE/MERGE again had colliding initial plans but different final joins. Native median 0.148932 s; broadcast 0.089038 s.
- `left_uniform_10_v1`: 28 correct runs, 20 measured, 12 million fact rows and 2 million unfiltered dimension rows (10% retained). Native median 1.275009 s; broadcast 0.241197 s; merge 2.174855 s; shuffle hash 1.336176 s. Broadcast beat native in all five randomized rounds. This is whole-query headroom, NOT a demonstrated runtime intervention or Jev policy gain. The cheap size rule also chooses broadcast.

## Transport experiment

`transport_probe.py` completed ten real requests against the existing pinned Jev endpoint. This includes two warmups and four randomized paired FRESH/POOLED blocks using exactly the same earlier synthetic runtime request. It is a latency experiment, not independent prediction evidence.

Fresh measured median round trip: 1.510792 s (range 0.939035–2.534077). Pooled: 0.496064 s (range 0.388587–0.642771). All four fresh requests established TCP/TLS; no measured pooled request did. Sample size is four per arm and server-side caching is unknown. Trace logs contain event names/times only, never headers or trace info.

Official TypeSafe API and model documentation were fetched by unauthenticated GET on the Mac, both HTTP 200, and saved with SHA-256 digests. The published endpoint and pinned model were found. The web-fetch tool had failed to retrieve these pages; the native documentation checks are separate evidence.

Cumulative durable request accounting is now **29/50**, with 21 attempts left. The ledger was not reset. Original frozen pilot and historical result files remain preserved.

## Next preregistered direction

Complete larger uniform and retained-skew development screens. Then freeze and directly measure an end-to-end pre-execution policy comparison on independently generated snapshots: native, an O(1) generator-metadata size rule, and the bounded pooled Jev client. Count actual candidate preparation, API failures/abstentions and execution in the Jev workflow. Do not present sums of separate medians as policy speedups. This is distinct from the actual runtime AQE observer and from production cluster validation.
