## Summary

Add a reproducible uv development environment and GitHub Actions validation
for every supported Python generation.

## Changes

- Add locked pytest, Ruff, and build development dependencies.
- Establish and satisfy the repository Ruff baseline.
- Add Python 3.10, 3.12, and 3.14 test jobs plus lint/build validation.
- Ignore generated environments, package metadata, caches, runtime databases,
  and the local recovered-image corpus.
- Document locked local commands and CI behavior.

## Validation

- `uv sync --frozen`
- `uv run --frozen ruff check .`
- `uv run --frozen pytest -q`: 22 passed.
- `uv run --frozen python -m build`: source and wheel distributions built.

## Checklist

- [x] Tests cover success and error or edge cases.
- [x] No product specification change requires a `docs/SPEC.md` update.
- [x] No user-visible product change requires a `CHANGELOG.md` update.
- [x] The PR file under `dev/PR/` matches the final branch diff.
