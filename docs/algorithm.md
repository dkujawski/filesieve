# Duplicate detection algorithm

`filesieve` identifies duplicate files using a content-derived hash key.

## Hash strategy

The algorithm is implemented in `filesieve.sieve` and works as follows:

1. Recursively walk each input base directory.
2. For each file, build a byte chunk used as hash input:
   - **Small file** (`size <= 2 * read_size`): read the entire file.
   - **Large file** (`size > 2 * read_size`): read only the first `read_size` bytes and last `read_size` bytes, then concatenate.
3. Compute an MD5 digest of that byte chunk.
4. If this digest has not been seen, keep the file in place.
5. If the digest has already been seen, treat file as duplicate and move it to `dup_dir` preserving a mirrored path.

Defaults:

- `read_size`: `1024` bytes
- `dup_dir`: `/tmp/sieve/dups`

Both values can be configured via `config/sieve.conf`.

## Why this strategy

Reading only head + tail data for larger files is much faster than hashing full large files, especially for media collections. It is a practical speed/accuracy tradeoff.

## Caveats and collision considerations

### Sampling caveat (most important)

For large files, only the first and last chunks are hashed. Two files with identical prefixes/suffixes but different middle content can be incorrectly identified as duplicates.

### MD5 collision caveat

MD5 is not collision-resistant for adversarial scenarios. In most non-malicious personal library cleanup workflows this is acceptable, but there is still a non-zero collision risk.

### Operational guidance

If false positives are unacceptable:

- Increase `read_size` in config.
- Add a secondary full-file verification step before moving duplicates.
- Consider replacing MD5 with a stronger digest (for example SHA-256) while retaining sampling or switching to full-file hashing.

## Behavior notes

- The "original" file retained is simply the first file encountered during directory traversal.
- Duplicate moves preserve path structure underneath `dup_dir`.
