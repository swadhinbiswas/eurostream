# Contributing

## Development setup

```bash
uv sync --dev        # Python deps
make gate            # full quality gate (lint, format, types, tests, contracts)
```

The quality gate must pass before any PR merges — CI enforces the same checks:

1. `ruff check src tests`
2. `ruff format --check src tests`
3. `mypy src/eurostream` (strict)
4. `pytest`
5. `eurostream contracts --baseline governance/contracts.json`

## Pre-commit hooks

```bash
make hooks          # uvx pre-commit install — runs from the repo's own venv
```

Every commit then gets the hygiene checks (JSON/TOML/YAML validity, private
keys, merge-conflict markers, trailing whitespace, end-of-file newline) plus
`ruff --fix` and `ruff format` on the staged files. `mypy` and the event
contract baseline are wired as **pre-push** hooks: they are the slow ones, and
they are the same commands the gate runs — no second toolchain to install and
no drift between what the hooks check and what CI checks. Run the whole set
over every file at any time with `uvx pre-commit run --all-files`.

## Tests and coverage

```bash
make coverage      # pytest with line coverage and the missing lines listed
```

Line coverage has a floor of **80%** (it sits at ~83%), configured once in
`[tool.coverage.report] fail_under` in `pyproject.toml`. It is not a target,
it is the line a change must not slip under: a deleted test fails, an
ordinary refactor does not. Any run that measures coverage honours it —
`make coverage`, the CI test job, and a hand-rolled
`pytest --cov=eurostream`. Raising the floor is welcome; lowering it needs a
reason in the PR.

Coverage measures effort, not correctness. What this repo leans on instead
is behavioural proof: every guardrail has a test that shows it firing
(retry, circuit breaker, dead-letter queue, hash chain, freshness/volume
gates), `eurostream chaos` fires six of them on purpose, and
`eurostream contracts` keeps the event schemas from moving quietly. The
code that stays below the line is the code that needs a network — the
Turso transport and the Hugging Face lake import — which is covered with
mocked transports where the logic matters and otherwise only runs against
the real thing.

## Changing event schemas

Event models in `src/eurostream/models.py` are **contracts**. After changing
one, regenerate the baseline and explain why the change is non-breaking:

```bash
uv run eurostream contracts --out governance/contracts.json
```

Removing a required field, making a required field optional, or changing a
type will fail CI until the baseline is consciously regenerated.

## Releasing

A release is: bump the version, move the changelog entries, publish the tag.

1. Set the same number in three places — `version` in `pyproject.toml`,
   `__version__` in `src/eurostream/__init__.py`, and the BibTeX citation in
   the README. Under `0.x` the minor is the major: a change to a documented
   contract (HTTP surface, CLI exit codes, event schemas, `EUROSTREAM_*`
   keys) means `0.4.0`, not `0.3.1`.
2. Move what has accumulated under `## [Unreleased]` in `CHANGELOG.md` into a
   new `## [X.Y.Z] - YYYY-MM-DD` section, **breaking changes first with the
   migration spelled out**, chore/CI/test noise left out.
3. Create a GitHub Release for tag `vX.Y.Z`.

`.github/workflows/release.yml` takes it from there: the full gate, `uv
build`, `twine check --strict`, a version-agreement check (tag == wheel
metadata == `__version__`), a check that `CHANGELOG.md` has a dated section
for exactly this version, then PyPI trusted publishing by OIDC. Trigger it
with `workflow_dispatch` to exercise everything except the upload.

## Docs site

The cookbook/docs live in `site/` (Astro + Starlight). Run locally with
`make site-dev`, build with `make site-build`.
