# Changelog

All notable user-visible changes to FileSieve are documented here.

## Unreleased

### Fixed

- Prevent repeated or overlapping scan roots from moving a file as its own
  duplicate.
- Keep organizer target trees out of source traversal so repeated runs remain
  idempotent when the target is nested below a source.
- Make organizer dry-runs read-only by deferring target-directory, state
  database, and default-config creation until an apply operation requires them.
- Compare perceptual hashes across exact prefix boundaries while retaining
  bounded candidate indexing.
- Compare image perceptual hashes across source resolutions so scaled copies can
  appear in the same similarity cluster.
- Reject similarity-report requests in exact-only mode with an actionable
  command-line error instead of writing an empty report.

### Added

- Add opt-in FFmpeg functional coverage for recovered image collections through
  the `FILESIEVE_RECOVERED_DIR` environment variable.
- Add `--move-similar` for explicitly moving perceptual duplicates while
  retaining the highest-resolution member of each cluster.
