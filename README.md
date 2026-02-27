# filesieve

`filesieve` is a command-line utility for finding duplicate files by content and moving duplicate copies into an alternate directory while leaving one original in place.

It started as a practical cleanup tool for merged media libraries where files often had different names but identical content.

## Project overview

- Walks one or more base directories recursively.
- Computes a hash key for each file's content sample.
- Keeps the first-seen file for each hash.
- Moves later files with the same hash to a configurable duplicate directory.

See [Duplicate detection algorithm](docs/algorithm.md) for implementation details and caveats.

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
filesieve [--alternate DUP_DIR] BASE_DIR [BASE_DIR ...]
```

### Examples

Use an explicit duplicate destination:

```bash
filesieve --alternate /tmp/sieve/dups ~/Music
```

Process multiple roots in one run:

```bash
filesieve --alternate /tmp/sieve/dups ~/Music ~/Videos
```

Run through `uv` without installing globally:

```bash
uv run filesieve --alternate /tmp/sieve/dups ./library
```

For full details, run:

```bash
filesieve --help
```


## configuration

You can pass an explicit config file with `--config /path/to/sieve.conf`.

Configuration precedence order is:

1. CLI args
2. config file values
3. in-code defaults

Supported config values in `[global]` are `read_size` and `dup_dir`.
`read_size` must be greater than `0` and `dup_dir` must be writable.


## Additional documentation

- [Duplicate detection algorithm and caveats](docs/algorithm.md)
- [Development workflow (setup, lint, test, build)](docs/development.md)
- [Release workflow (versioning + publishing)](docs/release.md)
- [Migration from legacy script usage](docs/migration.md)
