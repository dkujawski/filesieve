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

`uv.lock` pins the complete development environment. Use `uv sync --frozen`
when validating that the lock file is current without modifying it.

## Local commands

### CLI smoke test

```bash
uv run filesieve --help
```

### Lint

This project does not currently enforce a pinned linter dependency in `pyproject.toml`.
If you have Ruff available in your environment, run:

```bash
uv run --frozen ruff check .
```

### Test

```bash
uv run --frozen pytest
```

Tests marked `functional` may require optional system tools or local data. The
recovered-image FFmpeg test added by the file-safety branch can be run with:

```bash
FILESIEVE_RECOVERED_DIR="$PWD/recovered" uv run --frozen pytest -m functional
```

### Build distributions

```bash
uv run --frozen python -m build
```

This produces source and wheel artifacts in `dist/`.

## Continuous integration

GitHub Actions runs the locked test suite on Python 3.10, 3.12, and 3.14 for
pull requests and pushes to `master`. A separate job runs Ruff and builds both
distribution formats.
