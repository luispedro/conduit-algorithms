# Repository Guidelines

## Project Structure & Module Organization
Library code lives under `Data/Conduit/`, with public modules following their file paths, for example `Data/Conduit/Algorithms.hs` and `Data/Conduit/Algorithms/Async.hs`. Tests live in `tests/` with `Tests.hs` as the main entry point and `TestLZMA.hs` for LZMA-specific coverage. Benchmarks live in `bench/Bench.hs`, and `run-benchmarks.sh` prepares sample input and writes `bench.html`. Build metadata is maintained in `package.yaml` alongside the checked-in `conduit-algorithms.cabal`; Stack configuration lives in `stack.yaml`.

## Build, Test, and Development Commands
Install Stack first, then use the Makefile shortcuts or the underlying Stack commands:

- `make build` or `stack build`: compile the library and tests.
- `make check` or `stack test`: run the Tasty test suite.
- `make fast` / `make fastcheck`: quicker local builds using a separate work dir.
- `make bench` or `stack bench`: run Criterion benchmarks.
- `./run-benchmarks.sh`: generate benchmark input data and export `bench.html`.

On Ubuntu CI, tests install `libbz2-dev` and `liblzma-dev`; local environments need equivalent system libraries for compression-related builds.

## Coding Style & Naming Conventions
Use standard Haskell module naming: file paths must mirror module names under `Data.Conduit.*`. Follow the existing layout: spaces over tabs, 4-space indentation, aligned guards/case branches, and explicit type signatures for top-level definitions. Keep language pragmas at the top of each file. The project builds with `-Wall`; treat new warnings as regressions.

## Testing Guidelines
Tests use `tasty`, `tasty-hunit`, `tasty-quickcheck`, and Template Haskell grouping. Name unit tests `case_*` and property tests `prop_*` so `testGroupGenerator` picks them up automatically. Add focused regression tests beside the affected area, and run `stack test` before opening a PR. For concurrency or compression changes, include at least one end-to-end conduit test.

## Commit & Pull Request Guidelines
Recent history uses short uppercase prefixes such as `ENH`, `RFCT`, `BLD`, `DOC`, `TST`, `MIN`, and `RLS`. Follow that style, for example `TST Add regression for asyncXz`. Pull requests should summarize behavior changes, mention dependency updates, link the relevant issue when available, and list the commands you ran (`stack test`, `stack bench`, etc.). Screenshots are generally not relevant for this repository.
Update `ChangeLog` whenever a change affects public behavior, fixes a user-visible bug, or alters contributor-facing workflow.
