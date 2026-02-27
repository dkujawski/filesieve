# Release workflow

## Versioning

`filesieve` follows semantic versioning (`MAJOR.MINOR.PATCH`) and stores the package version in `pyproject.toml` under `[project].version`.

Suggested conventions:

- `PATCH`: bug fixes and documentation-only behavior clarifications.
- `MINOR`: backward-compatible features and CLI additions.
- `MAJOR`: breaking CLI or behavior changes.

## Publishing checklist

1. Ensure clean working tree and passing tests.
2. Bump version in `pyproject.toml`.
3. Build package artifacts:

   ```bash
   uv build
   ```

4. Validate generated artifacts in `dist/`.
5. Publish to package index (example using Twine):

   ```bash
   uv run twine upload dist/*
   ```

6. Tag release in git (for example `v0.1.0`) and push tags.

## Notes

- Keep release notes focused on user-visible CLI and duplicate-detection behavior changes.
- If duplicate detection logic changes, call out migration/compatibility implications explicitly.
