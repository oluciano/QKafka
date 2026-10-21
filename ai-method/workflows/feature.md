# Workflow: Feature

**When:** Implementing a new capability: consumer/producer behavior, middleware, outbox/inbox store, saga feature, dashboard feature, or public API.

---

## 1. Pre-Flight
- **Spec first:** a `specs/NN-<name>.md` file must exist (see `ROADMAP.md` phase). No spec, no code.
- **WIP Limit = 1:** only one task in progress (`gh issue list --label in-progress`, or the active spec).
- **Warmup Check:** inspect the target files on `main` to confirm the requirement is not already met.
- **Roadmap alignment:** the feature belongs to a `ROADMAP.md` phase / milestone; new features go in a MINOR (`vX.Y.0`), never a PATCH.
- **Branch Isolation:** branch off `main`: `git checkout -b feat/<id>-<description>`.

---

## 2. Technical Architecture & Invariants (Phase 0)
- Mode: `ai-method/modes/01-architect-mode.md`.
- Respect `core/00-foundation-minimal.md`: storage is the single source of truth; the poll loop never blocks; order per partition key; atomic saga state + outbox commit.
- No unnecessary DTOs or leaky Confluent types in the public API (`IKafkaContext`, `IKafkaConsumer<T>`, `IKafkaPublisher`).
- **Public API evolution:** add new overloads; never add optional parameters to existing interface methods.
- **Multi-targeting:** design for `net8.0` and `net10.0`; `#if NET10_0_OR_GREATER` only when necessary.
- Touching the consume path (`KafkaConsumerLoop`, `ChannelDispatcher`, `PartitionWorker`, `KafkaPipelineBuilder`) requires explicit architectural approval and the `reliability` workflow.

---

## 3. Mandatory 3N Testing Matrix & Red-First Proof (Phase 2)
Write the tests FIRST:
- **N1 — Positive:** happy path (message consumed, offset committed, event published).
- **N2 — Negative:** handled failures (handler throws → retry → DLT, DB down, broker disconnected).
- **N3 — Invalid Input:** null, empty payload, corrupt serialization, missing key, boundary values.
- **Red-First Proof:** run the new tests, watch them fail for the expected reason, commit them first (`test(scope): <description>, red`).
- **No Fixed Sleeps:** never `Task.Delay(50)` to wait for state. Use `TestWait.UntilAsync`, `TaskCompletionSource`, or an observed signal.
- **Test Contract Rule:** never weaken, invert or delete an assertion. Fix production code. One canonical `<Class>Tests.cs` per class; no `*HardeningTests.cs` twins.

---

## 4. Atomic Implementation (Phase 3)
- Mode: `ai-method/modes/02-execution-mode.md`.
- Implement ONLY what the spec's Definition of Done says.
- `sealed` by default; XML docs (`///`) on public APIs.
- async/await only, propagate `CancellationToken`, `.ConfigureAwait(false)` in `src/qKafka*`.
- Zero warnings (`TreatWarningsAsErrors = true`) on `net8.0` and `net10.0`.

---

## 5. Automated Verification & Gates
- `dotnet format --verify-no-changes`
- `dotnet build -c Release` (0 warnings, 0 errors)
- `dotnet test tests/<TouchedProject> --no-build` (scoped to the touched project)
- **Changelog:** entry under `## [Unreleased]` → `### Added`.
- **Docs truth:** update `README.md`, `docs/` and the spec/ROADMAP checkbox.
- **OTel:** new behavior that runs per message emits trace/metric data (invariant of the roadmap).

---

## 6. Exit Criteria
- [ ] Spec exists and the DoD is met
- [ ] Red-first proof committed
- [ ] 3N matrix satisfied, no fixed sleeps
- [ ] Zero warnings in Release on `net8.0` and `net10.0`
- [ ] `dotnet format` verified
- [ ] All tests pass; no existing test modified without approval
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] Docs and `ROADMAP.md` checkbox updated
