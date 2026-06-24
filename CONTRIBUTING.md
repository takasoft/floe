# Contributing to Floe

Thanks for your interest in improving Floe. This document explains how to set up
a development environment, run the checks, and propose changes.

Floe is an early-stage but actively maintained project. Bug reports, focused pull
requests, documentation fixes, and design discussion are all welcome.

## Ways to contribute

- Report a bug or unexpected behavior (open an issue with a minimal reproduction).
- Propose a feature or design change (open an issue first so we can agree on scope
  before you write code).
- Improve documentation, examples, or the quickstart.
- Pick up an issue labelled `good first issue` or `help wanted`.

## Development setup

Floe targets Python 3.11+ and uses [uv](https://github.com/astral-sh/uv) for
environment and dependency management.

```bash
# clone your fork
git clone https://github.com/<your-username>/floe.git
cd floe

# create the virtual environment and install Floe with dev (and optional) extras
uv venv
uv pip install -e ".[dev]"
# or, to include the Postgres catalog driver used by the Docker setup:
uv pip install -e ".[dev,postgres]"
```

If you prefer plain `pip`, an editable install with the same extras works too.

## Running the checks

All pull requests must keep the test suite and linter green. These are the same
commands CI runs:

```bash
# run the test suite
uv run pytest -q

# lint
uv run ruff check .

# auto-format and auto-fix where possible
uv run ruff format .
uv run ruff check --fix .
```

Please add or update tests for any behavior change. The suite lives in `tests/`
and runs entirely against local Iceberg tables, so it needs no external services.

## Pull request guidelines

1. Open an issue first for anything beyond a small fix, so the approach can be
   agreed before you invest time.
2. Keep each pull request focused on a single concern. Small, reviewable diffs
   merge faster.
3. Write a clear description: what changed, why, and how you verified it.
4. Make sure `pytest` and `ruff check` both pass locally.
5. Update the README, examples, or `CHANGELOG.md` when your change affects them.

## Commit and changelog conventions

- Write commit messages in the imperative mood (for example, "Add partitioned
  refresh window" rather than "Added ...").
- Note user-visible changes under the `Unreleased` section of `CHANGELOG.md`.

## Code of conduct

Participation in this project is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md). By taking part, you agree to uphold it.

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE), the same license that covers the project.
