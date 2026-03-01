import os

import pytest

from filesieve import media


def _meta(path: str, kind: str) -> media.MediaFileMeta:
    stat = os.stat(path)
    return media.MediaFileMeta(
        path=path,
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        dev=stat.st_dev,
        ino=stat.st_ino,
        kind=kind,
    )


def test_dhash_is_deterministic():
    pixels = bytes(range(72))
    first = media.dhash_from_pixels(pixels)
    second = media.dhash_from_pixels(pixels)
    assert first == second


def test_image_similarity_threshold_boundary(tmp_path, monkeypatch):
    left = tmp_path / "left.jpg"
    right = tmp_path / "right.jpg"
    left.write_bytes(b"left")
    right.write_bytes(b"right")

    hashes = {
        str(left): 0,
        str(right): (1 << 8) - 1,  # Hamming distance 8 from zero.
    }

    monkeypatch.setattr(media, "resolve_media_tools", lambda **kwargs: ("ffmpeg", "ffprobe"))

    def fake_image_signature(path, *, ffmpeg_bin, ffprobe_bin):
        return (
            {"kind": media.IMAGE_KIND, "hash": hashes[path]},
            {"width": 1000, "height": 1000, "duration": 0.0},
        )

    monkeypatch.setattr(media, "_image_signature", fake_image_signature)

    result = media.run_media_pipeline(
        [_meta(str(left), media.IMAGE_KIND), _meta(str(right), media.IMAGE_KIND)],
        moved_paths=set(),
        media_workers=1,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=None,
        ffprobe_path=None,
        cache=None,
        run_id="run-1",
    )

    assert len(result.similar_media_candidates) == 1
    assert sorted(result.similar_media_candidates[0]["paths"]) == sorted([str(left), str(right)])


def test_image_similarity_threshold_exclusive(tmp_path, monkeypatch):
    left = tmp_path / "left.jpg"
    right = tmp_path / "right.jpg"
    left.write_bytes(b"left")
    right.write_bytes(b"right")

    hashes = {
        str(left): 0,
        str(right): (1 << 9) - 1,  # Hamming distance 9 from zero.
    }

    monkeypatch.setattr(media, "resolve_media_tools", lambda **kwargs: ("ffmpeg", "ffprobe"))

    def fake_image_signature(path, *, ffmpeg_bin, ffprobe_bin):
        return (
            {"kind": media.IMAGE_KIND, "hash": hashes[path]},
            {"width": 1000, "height": 1000, "duration": 0.0},
        )

    monkeypatch.setattr(media, "_image_signature", fake_image_signature)

    result = media.run_media_pipeline(
        [_meta(str(left), media.IMAGE_KIND), _meta(str(right), media.IMAGE_KIND)],
        moved_paths=set(),
        media_workers=1,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=None,
        ffprobe_path=None,
        cache=None,
        run_id="run-1",
    )

    assert result.similar_media_candidates == []


def test_video_similarity_total_threshold_boundary(tmp_path, monkeypatch):
    left = tmp_path / "left.mp4"
    right = tmp_path / "right.mp4"
    left.write_bytes(b"left-video")
    right.write_bytes(b"right-video")

    signatures = {
        str(left): [0, 0, 0, 0],
        str(right): [(1 << 8) - 1, (1 << 8) - 1, (1 << 8) - 1, (1 << 8) - 1],
    }

    monkeypatch.setattr(media, "resolve_media_tools", lambda **kwargs: ("ffmpeg", "ffprobe"))

    def fake_video_signature(path, *, ffmpeg_bin, ffprobe_bin):
        return (
            {"kind": media.VIDEO_KIND, "hashes": signatures[path]},
            {"width": 1920, "height": 1080, "duration": 120.0},
        )

    monkeypatch.setattr(media, "_video_signature", fake_video_signature)

    result = media.run_media_pipeline(
        [_meta(str(left), media.VIDEO_KIND), _meta(str(right), media.VIDEO_KIND)],
        moved_paths=set(),
        media_workers=1,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=None,
        ffprobe_path=None,
        cache=None,
        run_id="run-1",
    )

    assert len(result.similar_media_candidates) == 1


def test_video_similarity_total_threshold_exclusive(tmp_path, monkeypatch):
    left = tmp_path / "left.mp4"
    right = tmp_path / "right.mp4"
    left.write_bytes(b"left-video")
    right.write_bytes(b"right-video")

    signatures = {
        str(left): [0, 0, 0, 0],
        str(right): [(1 << 9) - 1, (1 << 8) - 1, (1 << 8) - 1, (1 << 8) - 1],
    }

    monkeypatch.setattr(media, "resolve_media_tools", lambda **kwargs: ("ffmpeg", "ffprobe"))

    def fake_video_signature(path, *, ffmpeg_bin, ffprobe_bin):
        return (
            {"kind": media.VIDEO_KIND, "hashes": signatures[path]},
            {"width": 1920, "height": 1080, "duration": 120.0},
        )

    monkeypatch.setattr(media, "_video_signature", fake_video_signature)

    result = media.run_media_pipeline(
        [_meta(str(left), media.VIDEO_KIND), _meta(str(right), media.VIDEO_KIND)],
        moved_paths=set(),
        media_workers=1,
        image_hamming_threshold=8,
        video_hamming_threshold=32,
        video_frame_hamming_threshold=12,
        duration_bucket_seconds=2,
        ffmpeg_path=None,
        ffprobe_path=None,
        cache=None,
        run_id="run-1",
    )

    assert result.similar_media_candidates == []


def test_media_stage_skips_when_tools_unavailable(tmp_path, monkeypatch, caplog):
    sample = tmp_path / "sample.jpg"
    sample.write_bytes(b"x")

    monkeypatch.setattr(media, "resolve_media_tools", lambda **kwargs: (None, None))

    with caplog.at_level("WARNING"):
        result = media.run_media_pipeline(
            [_meta(str(sample), media.IMAGE_KIND)],
            moved_paths=set(),
            media_workers=1,
            image_hamming_threshold=8,
            video_hamming_threshold=32,
            video_frame_hamming_threshold=12,
            duration_bucket_seconds=2,
            ffmpeg_path=None,
            ffprobe_path=None,
            cache=None,
            run_id="run-1",
        )

    assert result.tools_available is False
    assert result.similar_media_candidates == []
    assert any("skipping perceptual media stage" in rec.message for rec in caplog.records)
