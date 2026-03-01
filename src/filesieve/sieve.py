"""Core duplicate-file detection orchestration for FileSieve."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import logging
import os
from configparser import ConfigParser
from time import perf_counter
import uuid

from filesieve.cache import SignatureCache
from filesieve.exact import ExactFileMeta, clean_dup, quick_hash, run_exact_pipeline
from filesieve.media import (
    IMAGE_KIND,
    VIDEO_KIND,
    MediaFileMeta,
    run_media_pipeline,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_DUP_DIR = "/tmp/sieve/dups"
DEFAULT_MODE = "media"
DEFAULT_CACHE_DB = ".filesieve-cache.sqlite"
DEFAULT_HASH_WORKERS = min(16, max(4, (os.cpu_count() or 1) * 2))
DEFAULT_MEDIA_WORKERS = max(2, (os.cpu_count() or 1) // 2)
DEFAULT_IMAGE_HAMMING_THRESHOLD = 8
DEFAULT_VIDEO_HAMMING_THRESHOLD = 32
DEFAULT_VIDEO_FRAME_HAMMING_THRESHOLD = 12
DEFAULT_DURATION_BUCKET_SECONDS = 2

IMAGE_EXTENSIONS = {
    ".bmp",
    ".gif",
    ".heic",
    ".heif",
    ".jpeg",
    ".jpg",
    ".png",
    ".tif",
    ".tiff",
    ".webp",
}
VIDEO_EXTENSIONS = {
    ".3gp",
    ".avi",
    ".flv",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ts",
    ".webm",
    ".wmv",
}


@dataclass(frozen=True)
class FileMeta:
    """Single file inventory record."""

    path: str
    size: int
    mtime_ns: int
    dev: int
    ino: int
    extension: str
    kind: str


@dataclass(frozen=True)
class ExactSignature:
    """Exact duplicate signatures for one file."""

    quick_hash: str | None = None
    full_hash: str | None = None


@dataclass(frozen=True)
class MediaSignature:
    """Perceptual media signature record for one file."""

    kind: str
    signature: tuple[int, ...]


@dataclass
class RunStats:
    """Mutable aggregate run stats."""

    files_scanned: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    bytes_read_exact: int = 0
    bytes_read_verify: int = 0
    timings_by_stage: dict[str, float] = field(default_factory=dict)

    @property
    def cache_hit_ratio(self) -> float:
        total = self.cache_hits + self.cache_misses
        if total <= 0:
            return 0.0
        return self.cache_hits / total

    def as_dict(self) -> dict[str, object]:
        return {
            "files_scanned": self.files_scanned,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_ratio": self.cache_hit_ratio,
            "bytes_read_exact": self.bytes_read_exact,
            "bytes_read_verify": self.bytes_read_verify,
            "timings_by_stage": dict(self.timings_by_stage),
        }


class Sieve:
    """Initialize config state and identify duplicates."""

    def __init__(
        self,
        *,
        config_path: str | None = None,
        dup_dir: str | None = None,
        mode: str | None = None,
        cache_db: str | None = None,
        no_cache: bool = False,
        hash_workers: int | None = None,
        media_workers: int | None = None,
        ffmpeg_path: str | None = None,
        ffprobe_path: str | None = None,
        image_hamming_threshold: int | None = None,
        video_hamming_threshold: int | None = None,
        video_frame_hamming_threshold: int | None = None,
        duration_bucket_seconds: int | None = None,
    ) -> None:
        config = self.__get_config(config_path)

        merged_dup_dir = DEFAULT_DUP_DIR
        merged_mode = DEFAULT_MODE
        merged_cache_db = DEFAULT_CACHE_DB
        merged_hash_workers = DEFAULT_HASH_WORKERS
        merged_media_workers = DEFAULT_MEDIA_WORKERS
        merged_media_enabled = True
        merged_ffmpeg_path = None
        merged_ffprobe_path = None
        merged_image_hamming = DEFAULT_IMAGE_HAMMING_THRESHOLD
        merged_video_hamming = DEFAULT_VIDEO_HAMMING_THRESHOLD
        merged_video_frame_hamming = DEFAULT_VIDEO_FRAME_HAMMING_THRESHOLD
        merged_duration_bucket_seconds = DEFAULT_DURATION_BUCKET_SECONDS

        if config is not None:
            merged_dup_dir = config.get("global", "dup_dir", fallback=merged_dup_dir)
            merged_mode = config.get("global", "mode", fallback=merged_mode)
            merged_cache_db = config.get("global", "cache_db", fallback=merged_cache_db)
            merged_hash_workers = int(
                config.get("global", "hash_workers", fallback=str(merged_hash_workers))
            )
            merged_media_workers = int(
                config.get("global", "media_workers", fallback=str(merged_media_workers))
            )

            merged_media_enabled = config.getboolean(
                "media", "enabled", fallback=merged_media_enabled
            )
            merged_ffmpeg_path = config.get(
                "media", "ffmpeg_path", fallback=merged_ffmpeg_path
            )
            merged_ffprobe_path = config.get(
                "media", "ffprobe_path", fallback=merged_ffprobe_path
            )
            merged_image_hamming = int(
                config.get(
                    "media",
                    "image_hamming_threshold",
                    fallback=str(merged_image_hamming),
                )
            )
            merged_video_hamming = int(
                config.get(
                    "media",
                    "video_hamming_threshold",
                    fallback=str(merged_video_hamming),
                )
            )
            merged_video_frame_hamming = int(
                config.get(
                    "media",
                    "video_frame_hamming_threshold",
                    fallback=str(merged_video_frame_hamming),
                )
            )
            merged_duration_bucket_seconds = int(
                config.get(
                    "media",
                    "duration_bucket_seconds",
                    fallback=str(merged_duration_bucket_seconds),
                )
            )

        if dup_dir is not None:
            merged_dup_dir = dup_dir
        if mode is not None:
            merged_mode = mode
        if cache_db is not None:
            merged_cache_db = cache_db
        if hash_workers is not None:
            merged_hash_workers = hash_workers
        if media_workers is not None:
            merged_media_workers = media_workers
        if ffmpeg_path is not None:
            merged_ffmpeg_path = ffmpeg_path
        if ffprobe_path is not None:
            merged_ffprobe_path = ffprobe_path
        if image_hamming_threshold is not None:
            merged_image_hamming = image_hamming_threshold
        if video_hamming_threshold is not None:
            merged_video_hamming = video_hamming_threshold
        if video_frame_hamming_threshold is not None:
            merged_video_frame_hamming = video_frame_hamming_threshold
        if duration_bucket_seconds is not None:
            merged_duration_bucket_seconds = duration_bucket_seconds

        self.dup_dir = self.__validate_dup_dir(merged_dup_dir)
        self.mode = self.__validate_mode(merged_mode)
        self.no_cache = bool(no_cache)
        self.cache_db = None if self.no_cache else self.__validate_cache_db(merged_cache_db)
        self.hash_workers = self.__validate_positive_int("hash_workers", merged_hash_workers)
        self.media_workers = self.__validate_positive_int("media_workers", merged_media_workers)
        self.media_enabled = bool(merged_media_enabled)
        self.ffmpeg_path = merged_ffmpeg_path
        self.ffprobe_path = merged_ffprobe_path
        self.image_hamming_threshold = self.__validate_non_negative_int(
            "image_hamming_threshold", merged_image_hamming
        )
        self.video_hamming_threshold = self.__validate_non_negative_int(
            "video_hamming_threshold", merged_video_hamming
        )
        self.video_frame_hamming_threshold = self.__validate_non_negative_int(
            "video_frame_hamming_threshold", merged_video_frame_hamming
        )
        self.duration_bucket_seconds = self.__validate_positive_int(
            "duration_bucket_seconds", merged_duration_bucket_seconds
        )

        self.results: dict[str, object] = {
            "duplicates_moved": [],
            "similar_media_candidates": [],
            "stats": {},
        }
        self.stats = RunStats()
        self.data: dict[str, list[str]] = {}

    def __get_config(self, config_path: str | None) -> ConfigParser | None:
        """Load an optional config file from an explicit deterministic path."""
        if config_path is None:
            return None

        resolved_path = os.path.abspath(config_path)
        if not os.path.exists(resolved_path):
            raise ValueError(f"Config file does not exist: {resolved_path}")

        config = ConfigParser()
        read_files = config.read(resolved_path)
        if not read_files:
            raise ValueError(f"Unable to read config file: {resolved_path}")
        return config

    def __validate_dup_dir(self, dup_dir: str) -> str:
        resolved_path = os.path.abspath(dup_dir)
        if os.path.exists(resolved_path) and not os.path.isdir(resolved_path):
            raise ValueError(
                f"Invalid config value for dup_dir: not a directory: {resolved_path}"
            )
        try:
            os.makedirs(resolved_path, exist_ok=True)
        except OSError as exc:
            raise ValueError(
                f"Invalid config value for dup_dir: cannot create directory {resolved_path}: {exc}"
            ) from exc
        if not os.access(resolved_path, os.W_OK):
            raise ValueError(
                f"Invalid config value for dup_dir: directory is not writable: {resolved_path}"
            )
        return resolved_path

    def __validate_mode(self, mode: str) -> str:
        if mode not in {"exact", "media"}:
            raise ValueError(f"Invalid mode {mode!r}; expected 'exact' or 'media'")
        return mode

    def __validate_cache_db(self, cache_db: str) -> str:
        resolved_path = os.path.abspath(cache_db)
        parent = os.path.dirname(resolved_path)
        if parent and not os.path.exists(parent):
            try:
                os.makedirs(parent, exist_ok=True)
            except OSError as exc:
                raise ValueError(
                    f"Invalid config value for cache_db: cannot create parent directory {parent}: {exc}"
                ) from exc
        return resolved_path

    def __validate_positive_int(self, field_name: str, value: int) -> int:
        if not isinstance(value, int):
            raise ValueError(f"Invalid config value for {field_name}: must be an integer")
        if value <= 0:
            raise ValueError(f"Invalid config value for {field_name}: must be greater than 0")
        return value

    def __validate_non_negative_int(self, field_name: str, value: int) -> int:
        if not isinstance(value, int):
            raise ValueError(f"Invalid config value for {field_name}: must be an integer")
        if value < 0:
            raise ValueError(f"Invalid config value for {field_name}: must be >= 0")
        return value

    @property
    def dup_count(self) -> int:
        """Return number of duplicates moved in the most recent run."""
        return len(self.results["duplicates_moved"])

    def walk(self, base_dir: str) -> dict[str, list[str]]:
        """Walk a single base directory."""
        return self.walk_many([base_dir])

    def walk_many(self, base_dirs: list[str]) -> dict[str, list[str]]:
        """Walk many base directories in one dedupe run."""
        if not isinstance(base_dirs, list) or not all(
            isinstance(base_dir, str) for base_dir in base_dirs
        ):
            raise TypeError("base_dirs must be a list of strings")

        self.results = {
            "duplicates_moved": [],
            "similar_media_candidates": [],
            "stats": {},
        }
        self.stats = RunStats()
        self.data = {}

        run_id = uuid.uuid4().hex
        cache: SignatureCache | None = None

        scan_start = perf_counter()
        files: list[FileMeta] = []
        for base_dir in base_dirs:
            if not os.path.exists(base_dir):
                LOGGER.error("Base directory tree does not exist: %s", base_dir)
                continue
            files.extend(self._scan_base_dir(base_dir))
        self.stats.timings_by_stage["scan"] = perf_counter() - scan_start
        self.stats.files_scanned = len(files)
        if not files:
            self.results["stats"] = self.stats.as_dict()
            return dict(self.data)

        if self.cache_db is not None:
            cache_open_start = perf_counter()
            cache = SignatureCache(self.cache_db)
            self.stats.timings_by_stage["cache_open"] = perf_counter() - cache_open_start

        exact_start = perf_counter()
        exact_result = run_exact_pipeline(
            [self._to_exact_meta(meta) for meta in files],
            dup_dir=self.dup_dir,
            hash_workers=self.hash_workers,
            cache=cache,
            run_id=run_id,
        )
        self.stats.timings_by_stage["exact"] = perf_counter() - exact_start
        self.results["duplicates_moved"] = exact_result.duplicates_moved
        self.stats.bytes_read_exact += exact_result.bytes_read_exact
        self.stats.bytes_read_verify += exact_result.bytes_read_verify
        self.stats.cache_hits += exact_result.cache_hits
        self.stats.cache_misses += exact_result.cache_misses

        media_start = perf_counter()
        if self.mode == "media" and self.media_enabled:
            media_result = run_media_pipeline(
                [self._to_media_meta(meta) for meta in files],
                moved_paths=exact_result.moved_paths,
                media_workers=self.media_workers,
                image_hamming_threshold=self.image_hamming_threshold,
                video_hamming_threshold=self.video_hamming_threshold,
                video_frame_hamming_threshold=self.video_frame_hamming_threshold,
                duration_bucket_seconds=self.duration_bucket_seconds,
                ffmpeg_path=self.ffmpeg_path,
                ffprobe_path=self.ffprobe_path,
                cache=cache,
                run_id=run_id,
            )
            self.results["similar_media_candidates"] = media_result.similar_media_candidates
            self.stats.cache_hits += media_result.cache_hits
            self.stats.cache_misses += media_result.cache_misses
        self.stats.timings_by_stage["media"] = perf_counter() - media_start

        if cache is not None:
            cache_commit_start = perf_counter()
            cache.commit()
            cache.prune_stale(run_id)
            cache.close()
            self.stats.timings_by_stage["cache_finalize"] = perf_counter() - cache_commit_start

        for meta in files:
            if meta.path in exact_result.moved_paths:
                continue
            self.data.setdefault(str(meta.size), []).append(meta.path)

        self.results["stats"] = self.stats.as_dict()
        return dict(self.data)

    def _scan_base_dir(self, base_dir: str) -> list[FileMeta]:
        """Inventory files recursively using iterative scandir traversal."""
        inventory: list[FileMeta] = []
        stack = [os.path.abspath(base_dir)]
        dup_dir_prefix = f"{self.dup_dir}{os.sep}"

        while stack:
            root = stack.pop()
            try:
                with os.scandir(root) as scan:
                    entries = sorted(list(scan), key=lambda entry: entry.name)
            except OSError:
                LOGGER.exception("Unable to scan directory: %s", root)
                continue

            dirs_to_visit: list[str] = []
            for entry in entries:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        dirs_to_visit.append(entry.path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    LOGGER.exception("Unable to stat path: %s", entry.path)
                    continue

                path = os.path.abspath(entry.path)
                if path == self.dup_dir or path.startswith(dup_dir_prefix):
                    continue

                extension = os.path.splitext(path)[1].lower()
                inventory.append(
                    FileMeta(
                        path=path,
                        size=stat.st_size,
                        mtime_ns=stat.st_mtime_ns,
                        dev=stat.st_dev,
                        ino=stat.st_ino,
                        extension=extension,
                        kind=self._classify_kind(extension),
                    )
                )
            stack.extend(reversed(dirs_to_visit))
        return inventory

    def _classify_kind(self, extension: str) -> str:
        if extension in IMAGE_EXTENSIONS:
            return IMAGE_KIND
        if extension in VIDEO_EXTENSIONS:
            return VIDEO_KIND
        return "other"

    def _to_exact_meta(self, meta: FileMeta) -> ExactFileMeta:
        return ExactFileMeta(
            path=meta.path,
            size=meta.size,
            mtime_ns=meta.mtime_ns,
            dev=meta.dev,
            ino=meta.ino,
        )

    def _to_media_meta(self, meta: FileMeta) -> MediaFileMeta:
        return MediaFileMeta(
            path=meta.path,
            size=meta.size,
            mtime_ns=meta.mtime_ns,
            dev=meta.dev,
            ino=meta.ino,
            kind=meta.kind,
        )


def process_file(file_path: str, read_size: int) -> str:
    """Backward-compatible helper for sampling and hashing one file."""
    if read_size <= 0:
        raise ValueError("read_size must be > 0")
    size = os.stat(file_path).st_size
    digest, _ = quick_hash(file_path, size=size, sample_size=read_size)
    return digest


def get_hash_key(data: bytes) -> str:
    """Backward-compatible helper: hash raw bytes with BLAKE2b."""
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("data must be bytes")
    return hashlib.blake2b(bytes(data), digest_size=16).hexdigest()
