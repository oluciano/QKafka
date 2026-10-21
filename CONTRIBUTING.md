# Contributing to qKafka

Thanks for your interest. Bug reports, ideas, documentation and code are all welcome.

## Before you start

- Search existing issues first.
- For anything that changes the public API or an invariant, open an issue before the pull request.
- Every task has a spec under `specs/` before coding (see `ai-method/`).

## Build and test

```bash
dotnet build --configuration Release   # zero warnings, zero errors
dotnet test  --configuration Release
dotnet format --verify-no-changes
```

Requires the .NET SDK pinned in `global.json` (10.0.100 or newer). Release builds use `TreatWarningsAsErrors`.

## Tests: the 3N matrix

Every feature or fix needs at least three tests:

1. **Positive:** the happy path.
2. **Negative:** a failure (database down, broker disconnected) behaves as specified.
3. **Invalid input:** null, empty, boundary or corrupt payload is handled without crashing.

Never edit or delete a passing test to make new code pass. Fix the code.

**Test file layout (enforced by a test):** `src/qKafka/<path>/Foo.cs` has exactly one unit test file `tests/qKafka.Tests/<path>/FooUnitTests.cs` (class `FooUnitTests`), and, in `tests/qKafka.ReliabilityTests`, one `<path>/FooReliabilityTests.cs`. Same relative path, no extra test files for the same class, no orphans. Files with only interfaces, enums or delegates need no test. Untested files with logic are tracked in `tests/qKafka.Tests/Conventions/untested-baseline.txt`, a list that only shrinks. Details in `ai-method/workflows/test.md`.

## Public API and versioning

qKafka is `0.x` until it meets the `1.0.0` criteria in [ROADMAP.md](ROADMAP.md#-versioning--api-stability).

- Types are `internal` by default. Make something public only when users must touch it.
- No `Confluent.Kafka` type in the public API.
- A public API change must be declared in `src/qKafka/PublicAPI.Unshipped.txt`. The build fails until it is.
- Breaking changes are allowed in a `0.x` minor but must be listed under `### Breaking` in the CHANGELOG. After `1.0`, mark `[Obsolete]` first and remove only in a major.
- New or unfinished surfaces use `[Experimental]`.

## Code style

`.editorconfig` and the analyzers are the source of truth: sealed classes by default, XML docs on public members, `CancellationToken` propagated, `ConfigureAwait(false)` in library code. Every rule turned off in `.editorconfig` carries its reason.

## Pull requests

- Branch from `main`, open the PR against `main`.
- Keep commits focused. No co-author or generated-by trailers.
- CI must pass: format, Release build, tests, vulnerability check, pack.
