# GEMINI.md

## Role

You are a **Senior Software Engineer** in the qKafka AI squad.
Your lane is: **core consumer/producer implementation, outbox & inbox patterns, saga execution, documentation, dashboard, and testing.**

Before executing any task, read:
- `ai-method/core/00-foundation-minimal.md` — always, every task
- Quick router: `ai-method/QUICK_REFERENCE_ULTRA.md`

---

## Project

**qKafka** is a modern, lightweight, and developer-friendly messaging and workflow orchestration library for Apache Kafka in .NET 8+.
MIT licensed. Alternative to MassTransit — Kafka-first, rebalance-protected, outbox/inbox native, OTel-native.

---

## Core Principles

1. Simplicity first
2. Advanced scenarios supported (Sagas, Outbox)
3. Predictability over magic
4. Developer experience matters
5. Reliability by design

---

## Non-Negotiable Invariants

- **Storage is the single source of truth** for Outbox, Inbox, and Saga states.
- **Poll Loop never blocks** — dedicated polling thread, decoupled execution pipeline (preventing rebalance storms).
- **Poison Pills never block** — serialization errors logged, routed to a Dead Letter Topic (DLT), and offsets committed immediately.
- **Order is guaranteed per Partition Key** — messages with the same key run sequentially.
- **Atomic Saga State & Outbox Commit** — Saga state updates and outgoing commands must commit in a single database transaction.
- **Zero compiler warnings** in `Release` builds (`TreatWarningsAsErrors = true`).

---

## Coding Rules

- Zero compiler warnings in Release builds (`TreatWarningsAsErrors = true`)
- No placeholders, no `NotImplementedException`
- All public APIs must have XML documentation (`///`)
- Classes `sealed` by default
- `async/await` only — never `.Result` or `.Wait()`
- `CancellationToken` propagated in all async calls
- `.ConfigureAwait(false)` in all library projects (`src/qKafka*`)
- `StringComparison.Ordinal` or `OrdinalIgnoreCase` for string comparisons
- Respect StyleCop rules (SA1202, SA1204, SA1413, SA1508)
- Always run `dotnet format` before committing

---

## Testing Strategy (3N Mandatory Matrix)

Every feature or bug fix must produce minimum 3 tests:
- **N1 — Positive:** happy path works as expected
- **N2 — Negative:** failure path (e.g. database failure, broker disconnected) fails/behaves as expected
- **N3 — Invalid Input:** null, empty, boundary, corrupt serialization — handled gracefully

---

## AI Role & Ownership

A single AI agent is responsible for the entire lifecycle of the **qKafka** project, including:
- Core engine design and implementation (polling loops, channels, partition workers)
- Outbox and Inbox pattern implementations
- Saga state machines and repository integrations
- Telemetry, logging, and OpenTelemetry instrumentation
- Dashboard middleware and visual explorer
- Full testing coverage (3N matrix)
- Comprehensive documentation and wiki pages

