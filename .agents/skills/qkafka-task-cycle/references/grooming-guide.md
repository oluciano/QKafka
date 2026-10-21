# Grooming guide — qKafka

Questions to answer before writing a spec. Write the answers into `specs/NN-*.md`.

## Consumption
- Does this touch the poll thread? Business logic must run only behind the `ChannelDispatcher`.
- What happens on rebalance mid-processing? Partition revoked: in-flight work finishes or is abandoned, and uncommitted offsets are redelivered.
- Is ordering per partition key preserved? Same key means sequential, different keys may run in parallel.
- When is the offset committed? Only after success, or after DLT routing succeeded.

## Failure handling
- Which errors are transient (retry with backoff) and which are permanent (DLT)? Serialization errors are poison pills: DLT and commit immediately.
- What does the DLT message carry (original topic, partition, offset, reason, exception)?
- What if the DLT publish itself fails? The offset must not be committed.

## Outbox / Inbox
- Is the outgoing message written in the same DB transaction as the state change?
- Is the Inbox check keyed by a stable message id? What is the retention?
- Does the outbox publisher stay safe with several instances (`SKIP LOCKED`)?

## Sagas
- Which message starts the saga, which correlates, which completes?
- Are state change and outgoing commands one transaction? Is concurrency optimistic (version column)?
- Compensation order (LIFO) and what happens if a compensation fails.

## Scope
- Public API change? XML docs, README and `docs/` impact.
- Multi-targeting (`net8.0`, `net10.0` if enabled in `Directory.Build.props`).
- Which ROADMAP phase and checkbox does this close? Is it a patch (bug fix only) or minor (new feature)?
