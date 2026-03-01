"""Perceptual media signature and similarity clustering pipeline."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import json
import logging
import os
import shutil
import subprocess
from typing import Callable, Iterable, TypeVar

from filesieve.cache import SignatureCache


LOGGER = logging.getLogger(__name__)

FRAME_WIDTH = 9
FRAME_HEIGHT = 8
FRAME_PIXELS = FRAME_WIDTH * FRAME_HEIGHT
VIDEO_FRACTIONS = (0.10, 0.35, 0.65, 0.90)
MAX_IN_FLIGHT_MULTIPLIER = 2

IMAGE_KIND = "image"
VIDEO_KIND = "video"


@dataclass(frozen=True)
class MediaFileMeta:
    """Metadata needed for perceptual-media processing."""

    path: str
    size: int
    mtime_ns: int
    dev: int
    ino: int
    kind: str


@dataclass
class MediaPipelineResult:
    """Output of media perceptual-stage processing."""

    similar_media_candidates: list[dict[str, object]]
    cache_hits: int
    cache_misses: int
    tools_available: bool


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


def _resolve_binary(binary: str | None, default_name: str) -> str | None:
    if binary:
        resolved = shutil.which(binary)
        if resolved:
            return resolved
        if os.path.isfile(binary):
            return os.path.abspath(binary)
        return None
    return shutil.which(default_name)


def resolve_media_tools(
    *,
    ffmpeg_path: str | None,
    ffprobe_path: str | None,
) -> tuple[str | None, str | None]:
    """Resolve FFmpeg/FFprobe executables."""
    ffmpeg_bin = _resolve_binary(ffmpeg_path, "ffmpeg")
    ffprobe_bin = _resolve_binary(ffprobe_path, "ffprobe")
    return ffmpeg_bin, ffprobe_bin


def hamming_distance(left: int, right: int) -> int:
    """Return bitwise Hamming distance between two integer hashes."""
    return (left ^ right).bit_count()


def dhash_from_pixels(
    pixels: bytes,
    *,
    width: int = FRAME_WIDTH,
    height: int = FRAME_HEIGHT,
) -> int:
    """Build a 64-bit dHash from grayscale pixels sized 9x8."""
    expected_len = width * height
    if len(pixels) < expected_len:
        raise ValueError(
            f"Not enough pixels for dHash: expected {expected_len}, got {len(pixels)}"
        )
    digest = 0
    for row in range(height):
        row_offset = row * width
        for col in range(width - 1):
            left = pixels[row_offset + col]
            right = pixels[row_offset + col + 1]
            digest = (digest << 1) | int(left > right)
    return digest


def _probe_media(path: str, *, ffprobe_bin: str) -> dict[str, float | int]:
    cmd = [
        ffprobe_bin,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height:format=duration",
        "-of",
        "json",
        path,
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "ffprobe failed")

    payload = json.loads(proc.stdout or "{}")
    stream = (payload.get("streams") or [{}])[0]
    width = int(stream.get("width") or 0)
    height = int(stream.get("height") or 0)
    duration_raw = (payload.get("format") or {}).get("duration")
    try:
        duration = float(duration_raw) if duration_raw is not None else 0.0
    except (TypeError, ValueError):
        duration = 0.0
    return {"width": width, "height": height, "duration": max(0.0, duration)}


def _extract_gray_frame(
    path: str,
    *,
    ffmpeg_bin: str,
    timestamp: float,
) -> bytes:
    cmd = [
        ffmpeg_bin,
        "-v",
        "error",
        "-ss",
        f"{timestamp:.3f}",
        "-i",
        path,
        "-vf",
        f"scale={FRAME_WIDTH}:{FRAME_HEIGHT}:flags=area,format=gray",
        "-frames:v",
        "1",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "gray",
        "pipe:1",
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or b"ffmpeg failed").decode("utf-8", errors="ignore"))
    if len(proc.stdout) < FRAME_PIXELS:
        raise RuntimeError("ffmpeg did not return enough frame bytes")
    return proc.stdout[:FRAME_PIXELS]


def _image_signature(path: str, *, ffmpeg_bin: str, ffprobe_bin: str) -> tuple[dict[str, object], dict[str, object]]:
    meta = _probe_media(path, ffprobe_bin=ffprobe_bin)
    frame = _extract_gray_frame(path, ffmpeg_bin=ffmpeg_bin, timestamp=0.0)
    signature = {
        "kind": IMAGE_KIND,
        "hash": dhash_from_pixels(frame),
    }
    return signature, meta


def _video_signature(path: str, *, ffmpeg_bin: str, ffprobe_bin: str) -> tuple[dict[str, object], dict[str, object]]:
    meta = _probe_media(path, ffprobe_bin=ffprobe_bin)
    duration = float(meta.get("duration", 0.0))
    timestamps = [duration * frac for frac in VIDEO_FRACTIONS] if duration > 0 else [0.0] * 4
    frame_hashes: list[int] = []
    for ts in timestamps:
        frame = _extract_gray_frame(path, ffmpeg_bin=ffmpeg_bin, timestamp=ts)
        frame_hashes.append(dhash_from_pixels(frame))
    signature = {
        "kind": VIDEO_KIND,
        "hashes": frame_hashes,
    }
    return signature, meta


def _blocking_key(
    *,
    signature: dict[str, object],
    meta: dict[str, object],
    duration_bucket_seconds: int,
) -> tuple[object, ...]:
    kind = str(signature["kind"])
    width = int(meta.get("width", 0))
    height = int(meta.get("height", 0))
    width_bucket = width // 64
    height_bucket = height // 64
    if kind == IMAGE_KIND:
        digest = int(signature["hash"])
        return (kind, width_bucket, height_bucket, digest >> 48)

    duration = float(meta.get("duration", 0.0))
    duration_bucket = (
        int(duration // duration_bucket_seconds) if duration_bucket_seconds > 0 else int(duration)
    )
    aspect_ratio_bucket = int(round((width / height) * 10)) if height else 0
    hashes = [int(value) for value in signature.get("hashes", [])]
    first_prefix = hashes[0] >> 48 if hashes else 0
    return (kind, duration_bucket, aspect_ratio_bucket, first_prefix)


class _UnionFind:
    def __init__(self, items: list[str]) -> None:
        self.parent = {item: item for item in items}
        self.rank = {item: 0 for item in items}

    def find(self, item: str) -> str:
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        rank_left = self.rank[root_left]
        rank_right = self.rank[root_right]
        if rank_left < rank_right:
            self.parent[root_left] = root_right
            return
        if rank_left > rank_right:
            self.parent[root_right] = root_left
            return
        self.parent[root_right] = root_left
        self.rank[root_left] += 1


def run_media_pipeline(
    files: list[MediaFileMeta],
    *,
    moved_paths: set[str],
    media_workers: int,
    image_hamming_threshold: int,
    video_hamming_threshold: int,
    video_frame_hamming_threshold: int,
    duration_bucket_seconds: int,
    ffmpeg_path: str | None,
    ffprobe_path: str | None,
    cache: SignatureCache | None,
    run_id: str,
) -> MediaPipelineResult:
    """Detect perceptual-similar media clusters (report-only)."""
    ffmpeg_bin, ffprobe_bin = resolve_media_tools(
        ffmpeg_path=ffmpeg_path,
        ffprobe_path=ffprobe_path,
    )
    if ffmpeg_bin is None or ffprobe_bin is None:
        LOGGER.warning(
            "FFmpeg tools unavailable; skipping perceptual media stage (ffmpeg=%s, ffprobe=%s)",
            ffmpeg_bin,
            ffprobe_bin,
        )
        return MediaPipelineResult(
            similar_media_candidates=[],
            cache_hits=0,
            cache_misses=0,
            tools_available=False,
        )

    candidates = [
        meta
        for meta in files
        if meta.path not in moved_paths and meta.kind in {IMAGE_KIND, VIDEO_KIND}
    ]
    if not candidates:
        return MediaPipelineResult(
            similar_media_candidates=[],
            cache_hits=0,
            cache_misses=0,
            tools_available=True,
        )

    cache_hits = 0
    cache_misses = 0
    signatures_by_path: dict[str, tuple[dict[str, object], dict[str, object]]] = {}
    todo: list[MediaFileMeta] = []

    for meta in candidates:
        if cache is not None:
            record = cache.get(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
            )
            if record is not None and record.media_sig and record.media_meta:
                try:
                    signature = json.loads(record.media_sig)
                    media_meta = json.loads(record.media_meta)
                except json.JSONDecodeError:
                    cache_misses += 1
                else:
                    cache_hits += 1
                    signatures_by_path[meta.path] = (signature, media_meta)
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
            else:
                cache_misses += 1
        todo.append(meta)

    def _compute_signature(meta: MediaFileMeta) -> tuple[dict[str, object], dict[str, object]] | None:
        try:
            if meta.kind == IMAGE_KIND:
                return _image_signature(meta.path, ffmpeg_bin=ffmpeg_bin, ffprobe_bin=ffprobe_bin)
            if meta.kind == VIDEO_KIND:
                return _video_signature(meta.path, ffmpeg_bin=ffmpeg_bin, ffprobe_bin=ffprobe_bin)
            return None
        except (RuntimeError, OSError, subprocess.SubprocessError) as exc:
            LOGGER.warning("Unable to compute media signature for %s: %s", meta.path, str(exc))
            return None

    for meta, result in _bounded_parallel_map(todo, _compute_signature, workers=media_workers):
        if result is None:
            continue
        signature, media_meta = result
        signatures_by_path[meta.path] = (signature, media_meta)
        if cache is not None:
            cache.upsert(
                path=meta.path,
                size=meta.size,
                mtime_ns=meta.mtime_ns,
                dev=meta.dev,
                ino=meta.ino,
                media_sig=json.dumps(signature, separators=(",", ":")),
                media_meta=json.dumps(media_meta, separators=(",", ":")),
                last_seen_run=run_id,
            )

    block_groups: dict[tuple[object, ...], list[str]] = defaultdict(list)
    for path, (signature, media_meta) in signatures_by_path.items():
        key = _blocking_key(
            signature=signature,
            meta=media_meta,
            duration_bucket_seconds=duration_bucket_seconds,
        )
        block_groups[key].append(path)

    uf = _UnionFind(list(signatures_by_path.keys()))
    score_by_pair: dict[frozenset[str], int] = {}

    for paths in block_groups.values():
        if len(paths) <= 1:
            continue
        ordered = sorted(paths)
        for idx, left_path in enumerate(ordered):
            left_signature, _ = signatures_by_path[left_path]
            for right_path in ordered[idx + 1 :]:
                right_signature, _ = signatures_by_path[right_path]
                if left_signature.get("kind") != right_signature.get("kind"):
                    continue
                kind = str(left_signature["kind"])
                similar = False
                score = 0
                if kind == IMAGE_KIND:
                    score = hamming_distance(
                        int(left_signature["hash"]),
                        int(right_signature["hash"]),
                    )
                    similar = score <= image_hamming_threshold
                elif kind == VIDEO_KIND:
                    left_hashes = [int(item) for item in left_signature.get("hashes", [])]
                    right_hashes = [int(item) for item in right_signature.get("hashes", [])]
                    if len(left_hashes) != len(right_hashes):
                        continue
                    frame_scores = [
                        hamming_distance(left_hashes[pos], right_hashes[pos])
                        for pos in range(len(left_hashes))
                    ]
                    score = sum(frame_scores)
                    similar = score <= video_hamming_threshold and all(
                        value <= video_frame_hamming_threshold for value in frame_scores
                    )
                if similar:
                    uf.union(left_path, right_path)
                    score_by_pair[frozenset({left_path, right_path})] = score

    components: dict[str, list[str]] = defaultdict(list)
    for path in signatures_by_path:
        components[uf.find(path)].append(path)

    similar_media_candidates: list[dict[str, object]] = []
    cluster_index = 0
    for paths in components.values():
        if len(paths) <= 1:
            continue
        cluster_index += 1
        cluster_paths = sorted(paths)
        pair_scores = [
            value
            for pair, value in score_by_pair.items()
            if pair.issubset(cluster_paths)
        ]
        kind = str(signatures_by_path[cluster_paths[0]][0].get("kind", "unknown"))
        score_summary = {
            "kind": kind,
            "pairs": len(pair_scores),
            "min": min(pair_scores) if pair_scores else 0,
            "max": max(pair_scores) if pair_scores else 0,
        }
        similar_media_candidates.append(
            {
                "cluster_id": f"media-{cluster_index}",
                "paths": cluster_paths,
                "score_summary": score_summary,
            }
        )

    return MediaPipelineResult(
        similar_media_candidates=similar_media_candidates,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        tools_available=True,
    )
