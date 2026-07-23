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

### Added

- Add opt-in FFmpeg functional coverage for recovered image collections through
  the `FILESIEVE_RECOVERED_DIR` environment variable.
