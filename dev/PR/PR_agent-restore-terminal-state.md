## Summary

Prevent media scans from leaving the invoking terminal with typed characters
hidden after FileSieve exits or is interrupted.

## Changes

- Disable FFmpeg interactive standard-input handling with `-nostdin`.
- Connect FFmpeg standard input to the null device instead of the invoking TTY.
- Add regression coverage for FFmpeg process isolation from terminal input.
- Document the CLI terminal-safety contract and user-visible fix.

## Validation

- `uv run --frozen pytest -q`: 35 passed, 3 opt-in functional tests skipped.
- `uv run --frozen ruff check .`: all checks passed.

## Checklist

- [x] Tests cover success and error or edge cases.
- [x] `docs/SPEC.md` reflects milestone or specification changes.
- [x] `CHANGELOG.md` reflects user-visible changes.
- [x] The PR file under `dev/PR/` matches the final branch diff.
