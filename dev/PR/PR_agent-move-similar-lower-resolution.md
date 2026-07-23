## Summary

Add an explicit perceptual-duplicate move mode that always retains the
highest-resolution member of each similarity cluster.

## Changes

- Add the media-only `--move-similar` CLI flag.
- Keep perceptual matching report-only unless the flag is supplied.
- Select canonical media by pixel area, then file size, oldest modification
  time, and lexicographic path.
- Move non-canonical cluster members into the configured alternate directory.
- Enrich cluster reports with retained and moved-file details.
- Add CLI, orchestration, canonical-selection, and recovered-image functional
  tests.

## Validation

- `uv run --frozen ruff check .`
- `uv run --frozen pytest -q`: 34 passed, 3 opt-in functional tests skipped.
- `FILESIEVE_RECOVERED_DIR="$PWD/recovered" uv run --frozen pytest -q
  test/test_media_functional.py`: 3 passed.

## Checklist

- [x] Tests cover success and error or edge cases.
- [x] `docs/SPEC.md` reflects the opt-in movement and canonical-selection rules.
- [x] `CHANGELOG.md` reflects the user-visible flag.
- [x] The PR file under `dev/PR/` matches the final branch diff.
