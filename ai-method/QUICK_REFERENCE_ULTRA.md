# qKafka AI Router

**Rule:** Every task MUST have a corresponding `.md` spec file under `specs/` before coding.

## Core Workflows
- **feature:** load foundation-minimal + feature specifications + validation matrices.
- **bugfix:** load foundation-minimal + bug analysis + regression test matrix.
- **refactor:** load foundation-minimal + code quality rules + unit test check.

## Non-Negotiable Invariants
- Separate poll thread & worker thread (rebalance protection).
- Sequential processing of matching keys.
- Idempotency guard via Inbox.
- Zero warnings in Release build.
- 3N test matrix mandatory (Positive, Negative, Invalid Input).
