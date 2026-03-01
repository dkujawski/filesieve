# Migration from legacy script usage

Historically, some workflows invoked FileSieve using repository-local wrappers (for example `bin/filesieve`) or module execution.

Current preferred usage is the installed console entry point:

```bash
filesieve [OPTIONS] BASE_DIR [BASE_DIR ...]
```

## Legacy to modern command mapping

### Legacy wrapper script

```bash
./bin/filesieve --alternate /tmp/sieve/dups ./library
```

### Equivalent console entry point

```bash
filesieve --alternate /tmp/sieve/dups ./library
```

### Module execution

```bash
python -m filesieve.cmd --alternate /tmp/sieve/dups ./library
```

### Equivalent via uv runner

```bash
uv run filesieve --alternate /tmp/sieve/dups ./library
```

## Migration notes

- Existing `--alternate` and positional base-path usage remains supported.
- New flags are available for media mode, cache behavior, worker tuning, and similarity reports.
- Console entry points simplify usage after installation and avoid hard-coding a local script path.
- CI and automation jobs should prefer `uv run filesieve ...` (repo context) or `filesieve ...` (installed tool context).
