# Duplicate detection algorithm

`filesieve` uses a staged pipeline optimized for large media libraries while preserving
zero-risk duplicate moves.

## Pipeline overview

1. Inventory scan:
   - Recursively enumerate files using iterative `os.scandir`.
   - Collect metadata: path, size, `mtime_ns`, `st_dev`, `st_ino`, extension, media kind.
   - Build size groups (`size -> files`) to avoid hashing singleton sizes.

2. Exact duplicate stage (always on):
   - For size groups with more than one file, compute `quick_hash`:
     - `BLAKE2b(digest_size=16)` over three 64 KiB samples at offsets:
       `0`, `size//2`, and `size-64KiB` (clamped).
   - For colliding quick-hash groups, compute streaming `full_hash`:
     - `BLAKE2b(digest_size=32)` over full bytes.
   - For colliding full-hash groups:
     - Keep canonical file = oldest `mtime_ns`, then lexicographic path.
     - Verify each candidate with chunked byte comparison (1 MiB chunks).
     - Move only when byte-compare succeeds.

3. Perceptual media stage (mode=`media` only):
   - Optional FFmpeg/FFprobe stage for images and video.
   - If tools are missing, stage is skipped and exact mode continues.
   - Image signature:
     - Decode one frame, scale to `9x8`, grayscale, compute 64-bit dHash.
   - Video signature:
     - Probe duration, sample frames at `10%`, `35%`, `65%`, `90%`.
     - Per frame: scale to `9x8`, grayscale, compute 64-bit dHash.
     - Combined signature is 4 x 64-bit hashes.
   - Candidate blocking:
     - Image key: `(width_bucket, height_bucket, hash_prefix16)`.
     - Video key: `(duration_bucket_2s, aspect_ratio_bucket, first_hash_prefix16)`.
   - Similarity thresholds:
     - Image: Hamming distance `<= image_hamming_threshold` (default `8`).
     - Video: total Hamming `<= video_hamming_threshold` (default `32`) and
       each frame `<= video_frame_hamming_threshold` (default `12`).
   - Output is report-only (`similar_media_candidates`). Perceptual matches are not moved.

4. Persistent signature cache:
   - SQLite cache stores exact and media signatures for repeated runs.
   - Cache identity requires unchanged `(path, size, mtime_ns, st_dev, st_ino)`.
   - Stale rows are pruned after each run.

## Default behavior

- `mode`: `media`
- `dup_dir`: `/tmp/sieve/dups`
- `cache_db`: `.filesieve-cache.sqlite`
- `hash_workers`: `min(16, max(4, cpu_count * 2))`
- `media_workers`: `max(2, cpu_count // 2)`
- `image_hamming_threshold`: `8`
- `video_hamming_threshold`: `32`
- `video_frame_hamming_threshold`: `12`
- `duration_bucket_seconds`: `2`

## Safety guarantees

- Exact duplicate moves require:
  1. same file size,
  2. same quick hash,
  3. same full hash,
  4. byte-for-byte verification.
- Perceptual-only matches are advisory and never auto-moved.
