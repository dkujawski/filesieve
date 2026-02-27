# Migration from legacy script usage

Historically, some workflows invoked FileSieve using repository-local wrappers (for example `bin/filesieve`) or module execution.

Current preferred usage is the installed console entry point:

```bash
filesieve [--alternate DUP_DIR] BASE_DIR [BASE_DIR ...]
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

- CLI flags and positional arguments are unchanged (`--alternate`, one or more base paths).
- Console entry points simplify usage after installation and avoid hard-coding a local script path.
- CI and automation jobs should prefer `uv run filesieve ...` (repo context) or `filesieve ...` (installed tool context).
