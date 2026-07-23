## Summary

Fix destructive duplicate and organizer edge cases, make dry-runs read-only,
and prevent perceptual similarity false negatives at hash-prefix boundaries.

## Changes

- De-duplicate repeated and overlapping scan paths before exact hashing.
- Exclude nested organizer targets from source traversal.
- Defer organizer state and target creation in dry-run mode.
- Use partitioned Hamming-space blocking for threshold-complete prefix
  candidate selection.
- Add regression tests and opt-in recovered-image FFmpeg functional coverage.

## Validation

- `python -m pytest -q`: 28 passed, 1 functional test skipped without corpus.
- `FILESIEVE_RECOVERED_DIR="$PWD/recovered" python -m pytest -q
  test/test_media_functional.py`: 1 passed.

## Checklist

- [x] Tests cover success and error or edge cases.
- [x] `docs/SPEC.md` reflects milestone or specification changes.
- [x] `CHANGELOG.md` reflects user-visible changes.
- [x] The PR file under `dev/PR/` matches the final branch diff.
