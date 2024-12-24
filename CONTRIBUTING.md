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

## Changing event schemas

Event models in `src/eurostream/models.py` are **contracts**. After changing
one, regenerate the baseline and explain why the change is non-breaking:

```bash
uv run eurostream contracts --out governance/contracts.json
```

Removing a required field, making a required field optional, or changing a
type will fail CI until the baseline is consciously regenerated.

## Docs site

The cookbook/docs live in `site/` (Astro + Starlight). Run locally with
`make site-dev`, build with `make site-build`.
