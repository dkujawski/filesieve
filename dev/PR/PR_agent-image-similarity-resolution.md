## Summary

Detect visually identical images across source resolutions and prevent
exact-only commands from silently writing empty perceptual reports.

## Changes

- Remove image dimensions from perceptual candidate block keys.
- Retain threshold-complete partitioned Hamming candidate selection.
- Reject `--report-similar` when the resolved mode is exact.
- Add unit regression coverage for scaled images and CLI validation.
- Add functional coverage for the scaled pair in `recovered/test_01`.

## Validation

- `uv run --frozen ruff check .`
- `uv run --frozen pytest -q`: 30 passed, 2 opt-in functional tests skipped.
- `FILESIEVE_RECOVERED_DIR="$PWD/recovered" uv run --frozen pytest -q
  test/test_media_functional.py`: 2 passed.

## Checklist

- [x] Tests cover success and error or edge cases.
- [x] `docs/SPEC.md` reflects the similarity and CLI specification changes.
- [x] `CHANGELOG.md` reflects user-visible changes.
- [x] The PR file under `dev/PR/` matches the final branch diff.
