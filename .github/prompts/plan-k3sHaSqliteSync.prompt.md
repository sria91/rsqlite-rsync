## Plan: K3s Active Passive SQLite HA

Build an active-passive SQLite service on k3s where exactly one pod is writer-active at a time, while node-local replicas are continuously synchronized using rsqlite-rsync. The core safety mechanism is lease-based single ownership plus fencing on promotion, and the core reliability mechanism is periodic plus lifecycle-triggered sync with freshness metadata.

**Steps**
1. Phase 1: Architecture and safety contract
2. Define ownership and fencing contract: one logical writer lease, one active node identity, one generation counter, and one freshness marker per sync cycle. Promotion is blocked unless lease is held and generation is current. This is the split-brain prevention baseline.
3. Define data paths and failure states for node-local database placement, bootstrap, steady-state sync, node loss, pod crash, network partition, and stale replica recovery. Mark this as the source-of-truth state machine for implementation and tests.
4. Reuse current snapshot semantics from origin and exclusive-write expectation at replica as non-negotiable invariants in cluster design.

5. Phase 2: Control plane integration in Kubernetes
6. Implement a leader election mechanism using Kubernetes Lease API (single owner) for the database writer pod identity.
7. Add a sidecar or companion process to run pull or push sync on interval plus preStop hook (depends on 6).
8. Add readiness gate logic so pod becomes ready only when ownership is confirmed and local DB has passed basic health checks (depends on 6, parallel with 7).
9. Add startup fence: if lease is not held, process must mount DB read-only or fail startup for writer role (depends on 6).

10. Phase 3: Sync orchestration and promotion workflow
11. Add sync scheduler policy: high-frequency incremental sync during steady state, forced sync on lifecycle events (preStop), and immediate catch-up on failover target node (depends on 7).
12. Persist freshness metadata: last successful sync timestamp, source generation, source node id, and checksum summary for observability and promotion gating (depends on 7).
13. Implement promotion gate: candidate node may start writer only if freshness age is under threshold and generation lineage is valid; otherwise require explicit recovery path (depends on 11 and 12).
14. Implement demotion behavior for old primary on lease loss: stop writes immediately and revert to replica-only mode (depends on 6).

15. Phase 4: Comprehensive test implementation
16. Unit tests for ownership and fencing logic: lease transitions, generation monotonicity, stale ownership rejection, and demotion-on-lease-loss behavior.
17. Unit tests for sync policy logic: interval scheduling, preStop trigger handling, backoff/retry behavior, and freshness threshold evaluation.
18. Integration tests in local cluster (k3d or k3s): bootstrap sync, steady-state sync under writes, failover with pod deletion, failover with node drain, and rejoin of old primary.
19. Fault-injection tests: network partition between active and standby, blocked sync transport, partial sync failures, and abrupt active node crash during write load.
20. Data-consistency tests: compare table row counts, per-page hash parity checks, and application-level invariants after each failover.
21. Concurrency safety tests: prove no dual-writer condition by validating lease ownership telemetry and write audit logs under rapid restarts.
22. Long-running soak test: 24h write workload with periodic forced failovers, tracking RPO distribution and any divergence incidents.
23. Performance tests: measure sync duration and transferred bytes for low, medium, and high churn profiles, and validate failover recovery time under each profile.

24. Phase 5: Operational hardening and rollout
25. Add metrics and alerts: sync success rate, sync lag seconds, lease holder changes, promotion attempts, stale-promotion denials, and replica freshness age.
26. Define SLO dashboards and alerts aligned to selected target (best-effort seconds-level RPO, low-seconds RTO under normal failover).
27. Stage rollout: dev cluster chaos tests, pre-prod canary with controlled failovers, then production phased rollout by environment.
28. Add runbooks for manual promotion, forced demotion, stale replica rebuild, and disaster recovery from last good snapshot.

**Relevant files**
- /Volumes/Developer/sria91/rsqlite-rsync/README.md — extend with k3s HA deployment model, lease/fencing assumptions, and operational guidance for single active pod usage.
- /Volumes/Developer/sria91/rsqlite-rsync/docs/protocol.md — document cluster-level constraints around replica exclusivity and failover sequencing expectations.
- /Volumes/Developer/sria91/rsqlite-rsync/src/main.rs — reference for CLI behavior used by sync sidecar and lifecycle hooks (dry run, local and remote modes).
- /Volumes/Developer/sria91/rsqlite-rsync/src/lib.rs — reference sync entry points and tuning knobs used by orchestration policy.
- /Volumes/Developer/sria91/rsqlite-rsync/tests/integration/live_writes.rs — baseline to extend into cluster failover with ongoing writes.
- /Volumes/Developer/sria91/rsqlite-rsync/tests/integration/local_sync.rs — baseline correctness expectations for full and incremental convergence.
- /Volumes/Developer/sria91/rsqlite-rsync/tests/integration/error_cases.rs — baseline error-path coverage to expand for orchestration and promotion failures.

**Verification**
1. Static checks: cargo build, cargo test, cargo clippy --all-targets --all-features -- -D warnings, cargo doc --no-deps.
2. Kubernetes functional suite: deploy 3-node k3s test environment, run synthetic write workload, verify single lease holder and no concurrent writer readiness.
3. Failover suite: terminate active pod, drain active node, and simulate node crash; record RPO and RTO for each.
4. Partition suite: isolate old primary from API server and peers, verify old primary demotes and cannot continue writes.
5. Recovery suite: bring old primary back, ensure it rejoins as replica and resyncs before eligibility for promotion.
6. Soak suite: continuous workload and scheduled failovers for 24h; assert zero split-brain events and bounded divergence.
7. Rollback drill: force rollback to previous release while preserving lease safety and data validity.

**Decisions**
- Storage: node-local disk path per node.
- Availability model: active-passive single writer, never dual writer.
- RPO/RTO target: best-effort seconds-level RPO, low-seconds recovery goal.
- Orchestration location: in-cluster Kubernetes control only (Lease and pod lifecycle integration).
- Included scope: ownership/fencing, sync orchestration, failover/promotion controls, full automated and chaos validation.
- Excluded scope: multi-writer SQLite and consensus-backed distributed SQL semantics.

**Further Considerations**
1. Freshness threshold policy recommendation: start with 5-10 seconds promotion threshold and tighten after soak-test evidence.
2. Sync cadence recommendation: 1-2 second interval under write load, plus mandatory preStop sync; tune by measured churn and CPU budget.
3. Optional hardening: add periodic full-byte equality verification during low-traffic windows to detect silent divergence.

**Execution Checklist (Implementation Handoff)**
1. Define CRD/config contract for HA controls: lease name, namespace, sync interval, freshness threshold, write mode.
2. Implement lease manager with explicit events: acquired, renewed, lost, stolen, expired.
3. Implement writer fence hook: reject writes unless lease state is acquired and generation is current.
4. Implement sync worker with modes: steady periodic, preStop forced, failover catch-up.
5. Implement freshness ledger (file or endpoint metadata): source node, source generation, sync completed timestamp, page/hash summary.
6. Implement promotion validator: allow promote only when lease acquired AND freshness age below threshold AND generation lineage valid.
7. Implement demotion behavior: on lease lost, close writable DB handle and switch to read-only/standby mode.
8. Implement observability: metrics and structured logs for lease and sync outcomes.
9. Add failure-injection hooks (config flags) to force sync failures, delayed acks, and stale freshness for testing.

**Test Matrix With Pass/Fail Criteria**
1. Test ID T01 (single owner bootstrap): start 3 nodes, deploy HA workload, verify exactly one lease holder and one writable pod. Pass if writable pod count never exceeds 1 for 10 minutes.
2. Test ID T02 (steady sync under writes): run sustained write workload 15 minutes. Pass if sync lag p95 <= threshold and no write errors on active node.
3. Test ID T03 (active pod deletion failover): delete active pod. Pass if new owner becomes writable within RTO target and divergence window <= freshness threshold.
4. Test ID T04 (node drain failover): drain active node. Pass if old owner stops writes before new owner accepts writes; zero overlap in writable window.
5. Test ID T05 (hard crash simulation): power off active node process. Pass if standby promotes after lease expiry and application resumes within target RTO.
6. Test ID T06 (network partition old primary): isolate old primary from API server/peers. Pass if old primary demotes and cannot continue writes.
7. Test ID T07 (sync path blocked): break transport for sync only. Pass if promotion of stale standby is denied and alerts fire.
8. Test ID T08 (partial sync failure): inject mid-transfer failure. Pass if retry converges without corruption and freshness metadata remains monotonic.
9. Test ID T09 (rapid restart churn): repeatedly restart pods and kubelet-managed containers. Pass if no dual-writer condition occurs.
10. Test ID T10 (old primary rejoin): restore old primary connectivity. Pass if it rejoins as replica and is not promotable before successful catch-up.
11. Test ID T11 (data integrity row/page): after each failover, run row-count and sampled-page-hash comparisons. Pass if all checks match expected lineage.
12. Test ID T12 (24h soak with chaos): periodic failovers and random faults over 24h. Pass if split-brain count = 0 and unresolved divergence incidents = 0.
13. Test ID T13 (rollback drill): roll back deployment version during write load. Pass if ownership safety and data correctness remain intact.

**CI and Staging Pipeline Order**
1. Stage A (PR fast): cargo build, targeted unit tests for lease/fencing/scheduler.
2. Stage B (PR standard): cargo test, cargo clippy --all-targets --all-features -- -D warnings, cargo doc --no-deps.
3. Stage C (nightly integration): ephemeral k3d/k3s 3-node run for T01-T05 and T09-T11.
4. Stage D (nightly chaos): run T06-T08 with fault injection.
5. Stage E (scheduled soak): run T12 daily or per release candidate.
6. Stage F (release gate): execute T13 plus curated failover scenarios; require manual sign-off.

**Artifacts Required Per Test Run**
1. Lease timeline log (owner identity over time).
2. Write audit log (node id, generation, write accepted/rejected).
3. Sync report (start/end timestamps, bytes transferred, pages changed, status).
4. Divergence report (row/hash comparisons and mismatch details).
5. RPO/RTO summary table with min/p50/p95/p99 values.

**Out-of-Scope Guardrails (Must Not Change)**
1. No multi-writer support.
2. No asynchronous promotion without lease confirmation.
3. No promotion when freshness exceeds threshold unless manual recovery mode is explicitly invoked.


**Go Live Scorecard (Ship or No Ship)**
1. Safety Gate S1: dual-writer incidents in all pre-release testing must equal 0. No ship if greater than 0.
2. Safety Gate S2: stale-promotion allowed events must equal 0. No ship if greater than 0.
3. Consistency Gate C1: divergence incidents after failover in T01-T13 must equal 0 unresolved. No ship if greater than 0 unresolved.
4. Availability Gate A1: failover RTO p95 must be <= target and p99 must not exceed 2x target. No ship if violated.
5. Durability Gate D1: measured RPO p95 must be <= freshness threshold and p99 within approved exception budget. No ship if violated.
6. Operability Gate O1: critical alerts (lease churn, sync stalled, stale promotion denied) must trigger correctly in test. No ship if any alert path fails.
7. Release Gate R1: rollback drill T13 must pass in release candidate build. No ship if rollback safety fails.

**Environment Promotion Criteria**
1. Dev to Pre-Prod: require T01-T05 and T09-T11 passing in two consecutive nightly runs.
2. Pre-Prod to Prod Canary: require T06-T08 passing plus at least one successful 24h soak T12.
3. Canary to Full Prod: require scorecard gates S1/S2/C1/A1/D1/O1/R1 all green for 7-day canary window.

**Incident Response Triggers and Actions**
1. Trigger: detected dual writer. Action: immediate write freeze, force demotion on all but lease owner, run divergence audit before unfreeze.
2. Trigger: stale promotion attempt. Action: deny promotion, raise critical alert, require forced resync and manual approval.
3. Trigger: sync lag exceeds threshold for sustained window. Action: mark standby non-promotable, increase sync diagnostics, evaluate transport saturation.
4. Trigger: repeated lease flaps. Action: freeze automated promotions, investigate API/server/network stability, resume only after stabilization criteria met.

**Release Sign Off Checklist**
1. Architecture sign-off: fencing and promotion gate behavior reviewed by two maintainers.
2. Test sign-off: latest matrix results attached with artifacts and percentile summaries.
3. SRE sign-off: alert routing, dashboard visibility, and runbook drills validated.
4. Product sign-off: accepted RPO/RTO expectations documented for stakeholders.
5. Final sign-off: explicit go decision recorded with rollback owner and on-call assignment.


**Execution Timeline (8 Weeks, Example)**
1. Week 1 Milestone: design freeze for safety model.
2. Deliverables: lease state machine spec, promotion and demotion contract, freshness metadata schema.
3. Exit criteria: architecture review approved by platform and SRE maintainers.
4. Owners: platform lead, database owner, SRE reviewer.

5. Week 2 Milestone: control path implementation.
6. Deliverables: lease manager, writer fence checks, readiness gate, startup guard.
7. Exit criteria: unit coverage for lease transitions and write rejection on non-owner paths.
8. Owners: platform engineer and application owner.

9. Week 3 Milestone: sync orchestration implementation.
10. Deliverables: periodic sync loop, preStop forced sync, failover catch-up behavior.
11. Exit criteria: integration happy-path tests pass for bootstrap and steady writes.
12. Owners: sync subsystem owner and QA engineer.

13. Week 4 Milestone: promotion safety hardening.
14. Deliverables: freshness ledger, generation lineage validation, stale promotion denial.
15. Exit criteria: T01-T05 and T09-T11 green in nightly runs.
16. Owners: platform owner and test owner.

17. Week 5 Milestone: fault handling and observability.
18. Deliverables: fault injection controls, alert rules, dashboards, structured events.
19. Exit criteria: T06-T08 pass and all critical alert routes confirmed.
20. Owners: SRE owner and platform engineer.

21. Week 6 Milestone: pre-prod canary.
22. Deliverables: staged deployment manifests, runbooks, incident drills.
23. Exit criteria: at least one successful 24h soak T12 with scorecard gates all green.
24. Owners: release manager, on-call SRE, QA lead.

25. Week 7 Milestone: production canary.
26. Deliverables: limited-traffic rollout, live telemetry review, rollback readiness.
27. Exit criteria: 7-day canary window with zero dual-writer and zero unresolved divergence.
28. Owners: release manager and operations lead.

29. Week 8 Milestone: full production rollout.
30. Deliverables: full environment enablement, final sign-offs, post-release report.
31. Exit criteria: scorecard remains green, rollback drill validated in release candidate branch.
32. Owners: platform team, SRE, product owner.

**RACI Snapshot**
1. Responsible: platform engineering for lease, fencing, and promotion logic.
2. Responsible: application team for DB write-path guards and readiness semantics.
3. Accountable: release manager for promotion decisions across environments.
4. Consulted: SRE for alerting, observability, and incident response runbooks.
5. Informed: product stakeholders for accepted RPO and RTO trade-offs.

**Critical Dependencies and Blockers**
1. Kubernetes API reliability for Lease heartbeats is a hard dependency.
2. Node time skew controls are required for consistent freshness-window evaluation.
3. Stable storage path and file permissions per node are required before integration testing.
4. Fault injection capability must be available in non-production environments.

**Contingency Paths**
1. If API instability causes lease flapping, pause automation and switch to controlled manual promotion with explicit approvals.
2. If sync lag exceeds thresholds under peak load, reduce write throughput or shorten batch sizes before widening promotion windows.
3. If soak uncovers intermittent divergence, block production rollout and enable periodic full-byte verification until root cause is closed.