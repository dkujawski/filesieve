from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from filesieve import media, sieve

pytestmark = pytest.mark.functional

RECOVERED_IMAGE_EXTENSIONS = {".jpeg", ".jpg", ".png", ".webp"}


def _meta(path: Path) -> media.MediaFileMeta:
    stat = path.stat()
    return media.MediaFileMeta(
        path=str(path),
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        dev=stat.st_dev,
        ino=stat.st_ino,
        kind=media.IMAGE_KIND,
    )


def test_recovered_images_decode_and_match_identical_copies(tmp_path):
    recovered_env = os.environ.get("FILESIEVE_RECOVERED_DIR")
    if recovered_env is None:
        pytest.skip("set FILESIEVE_RECOVERED_DIR to run recovered-image tests")

    recovered_dir = Path(recovered_env)
    recovered_candidates = sorted(
        path
        for path in recovered_dir.iterdir()
        if path.is_file() and path.suffix.lower() in RECOVERED_IMAGE_EXTENSIONS
    )

    ffmpeg_bin, ffprobe_bin = media.resolve_media_tools(
        ffmpeg_path=None,
        ffprobe_path=None,
    )
    if ffmpeg_bin is None or ffprobe_bin is None:
        pytest.skip("recovered-image tests require FFmpeg and FFprobe")

    samples: list[Path] = []
    for candidate in recovered_candidates:
        try:
            media._image_signature(
                str(candidate),
                ffmpeg_bin=ffmpeg_bin,
                ffprobe_bin=ffprobe_bin,
            )
        except (OSError, RuntimeError):
            continue
        samples.append(candidate)
        if len(samples) == 3:
            break
    if len(samples) < 3:
        pytest.skip("recovered-image tests require three decodable images")

    pairs: list[tuple[Path, Path]] = []
    candidates: list[media.MediaFileMeta] = []
    for index, source in enumerate(samples):
        original = tmp_path / f"sample-{index}{source.suffix.lower()}"
        duplicate = tmp_path / f"sample-{index}-copy{source.suffix.lower()}"
        shutil.copy2(source, original)
        shutil.copy2(source, duplicate)
        pairs.append((original, duplicate))
        candidates.extend((_meta(original), _meta(duplicate)))

    result = media.run_media_pipeline(
        candidates,
        moved_paths=set(),
        media_workers=2,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=ffmpeg_bin,
        ffprobe_path=ffprobe_bin,
        cache=None,
        run_id="functional-recovered-images",
    )

    clusters = [
        set(cluster["paths"]) for cluster in result.similar_media_candidates
    ]
    for original, duplicate in pairs:
        expected_pair = {str(original), str(duplicate)}
        assert any(expected_pair <= cluster for cluster in clusters)


def test_recovered_scaled_images_cluster():
    recovered_env = os.environ.get("FILESIEVE_RECOVERED_DIR")
    if recovered_env is None:
        pytest.skip("set FILESIEVE_RECOVERED_DIR to run recovered-image tests")

    fixture_dir = Path(recovered_env) / "test_01"
    samples = sorted(fixture_dir.glob("*.png"))
    if len(samples) != 2:
        pytest.skip("recovered/test_01 must contain exactly two PNG images")

    result = media.run_media_pipeline(
        [_meta(path) for path in samples],
        moved_paths=set(),
        media_workers=2,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=None,
        ffprobe_path=None,
        cache=None,
        run_id="functional-scaled-images",
    )

    assert len(result.similar_media_candidates) == 1
    assert set(result.similar_media_candidates[0]["paths"]) == {
        str(path) for path in samples
    }


def test_recovered_scaled_move_keeps_highest_resolution(tmp_path):
    recovered_env = os.environ.get("FILESIEVE_RECOVERED_DIR")
    if recovered_env is None:
        pytest.skip("set FILESIEVE_RECOVERED_DIR to run recovered-image tests")

    fixture_dir = Path(recovered_env) / "test_01"
    recovered_samples = sorted(fixture_dir.glob("*.png"))
    if len(recovered_samples) != 2:
        pytest.skip("recovered/test_01 must contain exactly two PNG images")

    source = tmp_path / "source"
    duplicate_dir = tmp_path / "duplicates"
    source.mkdir()
    samples: list[Path] = []
    for recovered_sample in recovered_samples:
        sample = source / recovered_sample.name
        shutil.copy2(recovered_sample, sample)
        samples.append(sample)

    ffmpeg_bin, ffprobe_bin = media.resolve_media_tools(
        ffmpeg_path=None,
        ffprobe_path=None,
    )
    if ffmpeg_bin is None or ffprobe_bin is None:
        pytest.skip("recovered-image tests require FFmpeg and FFprobe")
    dimensions = {
        path: media._probe_media(str(path), ffprobe_bin=ffprobe_bin)
        for path in samples
    }
    expected_kept = max(
        samples,
        key=lambda path: dimensions[path]["width"] * dimensions[path]["height"],
    )
    expected_moved = next(path for path in samples if path != expected_kept)

    engine = sieve.Sieve(
        mode="media",
        move_similar=True,
        dup_dir=str(duplicate_dir),
        no_cache=True,
        hash_workers=1,
        media_workers=2,
        ffmpeg_path=ffmpeg_bin,
        ffprobe_path=ffprobe_bin,
    )
    engine.walk(str(source))

    assert expected_kept.exists()
    assert not expected_moved.exists()
    assert engine.dup_count == 1
    move = engine.results["duplicates_moved"][0]
    assert move["kept"] == str(expected_kept)
    assert move["source"] == str(expected_moved)
    assert move["reason"] == "perceptual-similar"
