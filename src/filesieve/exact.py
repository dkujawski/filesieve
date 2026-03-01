"""Exact duplicate detection pipeline primitives."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import hashlib
import logging
import os
import shutil
from typing import Callable, Iterable, TypeVar

from filesieve.cache import SignatureCache


LOGGER = logging.getLogger(__name__)

QUICK_SAMPLE_SIZE = 64 * 1024
HASH_CHUNK_SIZE = 1024 * 1024
MAX_IN_FLIGHT_MULTIPLIER = 4


@dataclass(frozen=True)
class ExactFileMeta:
    """Minimal file metadata needed by exact hashing."""

    path: str
    size: int
    mtime_ns: int
    dev: int
    ino: int


@dataclass
class ExactPipelineResult:
    """Aggregate exact-stage output and metrics."""

    duplicates_moved: list[dict[str, str]]
    moved_paths: set[str]
    bytes_read_exact: int
    bytes_read_verify: int
    cache_hits: int
    cache_misses: int


T = TypeVar("T")
R = TypeVar("R")


def _bounded_parallel_map(
    items: Iterable[T],
    fn: Callable[[T], R],
    *,
    workers: int,
) -> list[tuple[T, R]]:
    item_list = list(items)
    if not item_list:
        return []
    if workers <= 1:
        return [(item, fn(item)) for item in item_list]

    max_in_flight = max(1, workers * MAX_IN_FLIGHT_MULTIPLIER)
    results: list[tuple[T, R]] = []
    iterator = iter(item_list)
    futures: dict[Future[R], T] = {}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for _ in range(min(max_in_flight, len(item_list))):
            item = next(iterator, None)
            if item is None:
                break
            futures[pool.submit(fn, item)] = item

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for fut in done:
                item = futures.pop(fut)
                results.append((item, fut.result()))

                next_item = next(iterator, None)
                if next_item is not None:
                    futures[pool.submit(fn, next_item)] = next_item
    return results


def _clamp_offset(offset: int, *, size: int, sample_size: int) -> int:
    max_start = max(0, size - sample_size)
    return min(max(offset, 0), max_start)


def quick_hash(path: str, *, size: int, sample_size: int = QUICK_SAMPLE_SIZE) -> tuple[str, int]:
    """Compute a BLAKE2b digest from 3 strategic samples."""
    offsets = [
        _clamp_offset(0, size=size, sample_size=sample_size),
        _clamp_offset(size // 2, size=size, sample_size=sample_size),
        _clamp_offset(size - sample_size, size=size, sample_size=sample_size),
    ]
    unique_offsets = list(dict.fromkeys(offsets))

    hasher = hashlib.blake2b(digest_size=16)
    bytes_read = 0
    with open(path, "rb") as fh:
        for offset in unique_offsets:
            fh.seek(offset, os.SEEK_SET)
            chunk = fh.read(sample_size)
            bytes_read += len(chunk)
            hasher.update(chunk)
    return hasher.hexdigest(), bytes_read


def full_hash(path: str, *, chunk_size: int = HASH_CHUNK_SIZE) -> tuple[str, int]:
    """Compute a streaming BLAKE2b digest over full file bytes."""
    hasher = hashlib.blake2b(digest_size=32)
    bytes_read = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            bytes_read += len(chunk)
            hasher.update(chunk)
    return hasher.hexdigest(), bytes_read


def compare_files(path_a: str, path_b: str, *, chunk_size: int = HASH_CHUNK_SIZE) -> tuple[bool, int]:
    """Compare two files chunk-by-chunk."""
    bytes_read = 0
    with open(path_a, "rb") as fh_a, open(path_b, "rb") as fh_b:
        while True:
            chunk_a = fh_a.read(chunk_size)
            chunk_b = fh_b.read(chunk_size)
            bytes_read += len(chunk_a) + len(chunk_b)
            if chunk_a != chunk_b:
                return False, bytes_read
            if not chunk_a:
                return True, bytes_read


def _mirror_destination(source_file: str, dup_dir: str) -> str:
    source_abs = os.path.abspath(source_file)
    drive, tail = os.path.splitdrive(source_abs)
    tail = tail.lstrip(os.sep).lstrip("/")
    if drive:
        drive_token = drive.rstrip(":").replace(":", "")
        rel_path = os.path.join(f"drive_{drive_token}", tail)
    else:
        rel_path = tail
    return os.path.join(dup_dir, rel_path)


def clean_dup(dup_file: str, dup_dir: str) -> str:
    """Move ``dup_file`` into a mirrored directory rooted in ``dup_dir``."""
    dest = _mirror_destination(dup_file, dup_dir)
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.move(dup_file, dest)
    return dest


def _build_size_groups(files: Iterable[ExactFileMeta]) -> dict[int, list[ExactFileMeta]]:
    size_groups: dict[int, list[ExactFileMeta]] = defaultdict(list)
    for meta in files:
        size_groups[meta.size].append(meta)
    return size_groups


def run_exact_pipeline(
    files: list[ExactFileMeta],
    *,
    dup_dir: str,
    hash_workers: int,
    cache: SignatureCache | None,
    run_id: str,
) -> ExactPipelineResult:
    """Run exact duplicate pipeline with staged hashing and byte verification."""
    duplicates_moved: list[dict[str, str]] = []
    moved_paths: set[str] = set()
    bytes_read_exact = 0
    bytes_read_verify = 0
    cache_hits = 0
    cache_misses = 0

    size_groups = _build_size_groups(files)
    candidate_files = [
        meta for group in size_groups.values() if len(group) > 1 for meta in group
    ]
    if not candidate_files:
        return ExactPipelineResult(
            duplicates_moved=duplicates_moved,
            moved_paths=moved_paths,
            bytes_read_exact=bytes_read_exact,
            bytes_read_verify=bytes_read_verify,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
        )

    quick_hashes: dict[str, str] = {}
    quick_todo: list[ExactFileMeta] = []

    for meta in candidate_files:
        if cache is not None:
            record = cache.get(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
            )
            if record is not None and record.quick_hash is not None:
                cache_hits += 1
                quick_hashes[meta.path] = record.quick_hash
                cache.upsert(
                    path=meta.path,
                    size=meta.size,
                    mtime_ns=meta.mtime_ns,
                    dev=meta.dev,
                    ino=meta.ino,
                    quick_hash=record.quick_hash,
                    full_hash=record.full_hash,
                    media_sig=record.media_sig,
                    media_meta=record.media_meta,
                    last_seen_run=run_id,
                )
                continue
            cache_misses += 1
        quick_todo.append(meta)

    def _compute_quick(meta: ExactFileMeta) -> tuple[str, int]:
        return quick_hash(meta.path, size=meta.size)

    for meta, (digest, read_bytes) in _bounded_parallel_map(
        quick_todo,
        _compute_quick,
        workers=hash_workers,
    ):
        quick_hashes[meta.path] = digest
        bytes_read_exact += read_bytes
        if cache is not None:
            cache.upsert(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
                quick_hash=digest,
                last_seen_run=run_id,
            )

    quick_groups: dict[tuple[int, str], list[ExactFileMeta]] = defaultdict(list)
    for meta in candidate_files:
        quick_groups[(meta.size, quick_hashes[meta.path])].append(meta)

    full_candidates = [
        meta for group in quick_groups.values() if len(group) > 1 for meta in group
    ]
    full_hashes: dict[str, str] = {}
    full_todo: list[ExactFileMeta] = []

    for meta in full_candidates:
        if cache is not None:
            record = cache.get(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
            )
            if record is not None and record.full_hash is not None:
                cache_hits += 1
                full_hashes[meta.path] = record.full_hash
                cache.upsert(
                    path=meta.path,
                    size=meta.size,
                    mtime_ns=meta.mtime_ns,
                    dev=meta.dev,
                    ino=meta.ino,
                    quick_hash=record.quick_hash,
                    full_hash=record.full_hash,
                    media_sig=record.media_sig,
                    media_meta=record.media_meta,
                    last_seen_run=run_id,
                )
                continue
            cache_misses += 1
        full_todo.append(meta)

    def _compute_full(meta: ExactFileMeta) -> tuple[str, int]:
        return full_hash(meta.path)

    for meta, (digest, read_bytes) in _bounded_parallel_map(
        full_todo,
        _compute_full,
        workers=hash_workers,
    ):
        full_hashes[meta.path] = digest
        bytes_read_exact += read_bytes
        if cache is not None:
            cache.upsert(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
                full_hash=digest,
                last_seen_run=run_id,
            )

    full_groups: dict[tuple[int, str], list[ExactFileMeta]] = defaultdict(list)
    for meta in full_candidates:
        full_groups[(meta.size, full_hashes[meta.path])].append(meta)

    for group in full_groups.values():
        if len(group) <= 1:
            continue
        ordered = sorted(group, key=lambda item: (item.mtime_ns, item.path))
        canonical = ordered[0]
        for candidate in ordered[1:]:
            is_equal, read_bytes = compare_files(canonical.path, candidate.path)
            bytes_read_verify += read_bytes
            if not is_equal:
                LOGGER.warning(
                    "Hash collision anomaly detected; skipping move for %s", candidate.path
                )
                continue
            try:
                destination = clean_dup(candidate.path, dup_dir)
            except OSError:
                LOGGER.exception("Unable to move duplicate file: %s", candidate.path)
                continue
            moved_paths.add(candidate.path)
            duplicates_moved.append(
                {
                    "source": candidate.path,
                    "destination": destination,
                    "kept": canonical.path,
                }
            )

    return ExactPipelineResult(
        duplicates_moved=duplicates_moved,
        moved_paths=moved_paths,
        bytes_read_exact=bytes_read_exact,
        bytes_read_verify=bytes_read_verify,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
    )
