# Verification gate — qKafka

Run in this order; stop at the first failure and fix the production code.

1. `dotnet format --verify-no-changes`
2. `dotnet build -c Release` — **zero warnings** (`TreatWarningsAsErrors = true`).
3. `dotnet test -c Release` — all tests green.
4. Spec check: every item of the spec's DoD is implemented; every 3N cell has a test.
5. Invariants check:
   - poll loop never runs business logic;
   - per-key order kept;
   - poison pill goes to the DLT and the offset is committed;
   - saga state and outbox rows commit in one transaction;
   - no `.Result`, `.Wait()`, `NotImplementedException`; `CancellationToken` propagated.
6. Grep the diff for leftovers: `TODO`, `Console.WriteLine`, commented-out code.

Report: commands run, real results, and anything not run (for example integration tests that need Docker or a Kafka broker).
