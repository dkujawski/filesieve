# Development

## Environment setup (with `uv`)

Create and activate a virtual environment:

```bash
uv venv
source .venv/bin/activate
```

Install project and development dependencies from `pyproject.toml`:

```bash
uv sync
```

## Local commands

### CLI smoke test

```bash
uv run filesieve --help
```

### Lint

This project does not currently enforce a pinned linter dependency in `pyproject.toml`.
If you have Ruff available in your environment, run:

```bash
uv run ruff check .
```

### Test

```bash
uv run pytest
```

### Build distributions

```bash
uv build
```

This produces source and wheel artifacts in `dist/`.
