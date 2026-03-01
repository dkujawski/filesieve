# filesieve

`filesieve` is a command-line utility for finding exact duplicate files and
moving duplicate copies into an alternate directory while leaving one canonical
original in place.

It is optimized for large media collections with:

- staged exact hashing (size filter -> quick hash -> full hash -> byte verify),
- optional perceptual media similarity clustering (images + video),
- persistent SQLite signatures cache for faster repeated runs.

## Project overview

- Walks one or more base directories recursively.
- Moves only exact byte-identical duplicates.
- Keeps canonical file by oldest `mtime_ns`, then lexicographic path.
- Emits perceptual media clusters as report-only output (no auto-move).

See [Duplicate detection algorithm](docs/algorithm.md) for details.

## Supported Python versions

`filesieve` currently supports **Python 3.10+**.

## Installation (with `uv`)

### Install from local source

```bash
uv sync
```

### Install as a tool (console entry point)

```bash
uv tool install .
```

After install, the `filesieve` command is available in your shell.

## CLI usage

General form:

```bash
filesieve [OPTIONS] BASE_DIR [BASE_DIR ...]
```

### Core options

- `-a, --alternate DUP_DIR`: move exact duplicates here.
- `-c, --config FILE`: optional config path.
- `--mode {exact,media}`: duplicate mode (`media` default).
- `--cache PATH`: SQLite cache path override.
- `--no-cache`: disable persistent cache.
- `--hash-workers N`: worker threads for exact hashing.
- `--media-workers N`: worker threads for perceptual media stage.
- `--ffmpeg PATH`: explicit `ffmpeg` path or executable name.
- `--ffprobe PATH`: explicit `ffprobe` path or executable name.
- `--report-similar PATH`: write perceptual media clusters JSON.

### Examples

Exact duplicate cleanup only:

```bash
filesieve --mode exact --alternate /tmp/sieve/dups ~/Videos
```

Media mode with report output:

```bash
filesieve --mode media --report-similar ./similar.json --alternate /tmp/sieve/dups ~/Photos ~/Videos
```

Run through `uv`:

```bash
uv run filesieve --alternate /tmp/sieve/dups ./library
```

For full details:

```bash
filesieve --help
```

## Configuration

Pass a config with `--config /path/to/sieve.conf`.

Precedence order:

1. CLI args
2. config file values
3. in-code defaults

Example config:

```ini
[global]
dup_dir:/tmp/sieve/dups
mode:media
cache_db:.filesieve-cache.sqlite
hash_workers:8
media_workers:2

[media]
enabled:true
image_hamming_threshold:8
video_hamming_threshold:32
video_frame_hamming_threshold:12
duration_bucket_seconds:2
```

## Safety model

- File moves happen only for exact duplicates after byte-for-byte verification.
- Perceptual matches are advisory in `similar_media_candidates` output only.
- If FFmpeg tools are unavailable, perceptual stage is skipped automatically.

## Additional documentation

- [Duplicate detection algorithm and caveats](docs/algorithm.md)
- [Development workflow (setup, lint, test, build)](docs/development.md)
- [Release workflow (versioning + publishing)](docs/release.md)
- [Migration from legacy script usage](docs/migration.md)
