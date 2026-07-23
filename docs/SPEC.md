# FileSieve specification

## Exact duplicate safety

- Every physical scan path is considered at most once per run, even when input
  roots repeat or overlap.
- Exact duplicates must have equal sizes, staged hashes, and byte content before
  FileSieve moves a non-canonical copy.
- A candidate path must never be compared with or moved as a duplicate of
  itself.

## Organizer behavior

- Dry-run mode calculates and reports operations without creating target
  directories, configuration files, or persistent state.
- Organizer source traversal excludes the complete target tree. This applies
  when the target is nested inside a source and preserves repeated-run
  idempotency.
- Apply mode records completed operations in the organizer state database.

## Perceptual media reporting

- Image and video similarity remains report-only.
- Hamming-space candidate indexing partitions signatures into
  `threshold + 1` segments. By the pigeonhole principle, hashes within the
  configured threshold share at least one segment and reach the final distance
  comparison even when their former 16-bit prefixes differ.
- Image candidate indexing is independent of source dimensions so resized
  copies can be compared. Video duration/aspect blocking remains in place.
- `--report-similar` requires media mode. Exact-only mode fails with an
  actionable error rather than producing a misleading empty report.
- Perceptual matches remain report-only unless `--move-similar` is explicitly
  supplied in media mode.
- When moving a similarity cluster, FileSieve keeps the greatest pixel-area
  member. Ties prefer greater file size, oldest `mtime_ns`, then
  lexicographically first path.
- Each moved perceptual duplicate records its source, destination, retained
  canonical path, and `perceptual-similar` reason in results and reports.
- Recovered-image functional tests are opt-in and require FFmpeg, FFprobe, and
  `FILESIEVE_RECOVERED_DIR`.
