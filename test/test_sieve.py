import os

import pytest

from filesieve import sieve


def test_walk_moves_newer_exact_duplicate_and_records_kept(tmp_path):
    src = tmp_path / "src"
    dup = tmp_path / "dups"
    src.mkdir()
    dup.mkdir()

    older = src / "older.bin"
    newer = src / "newer.bin"
    payload = b"same-content" * 2048
    older.write_bytes(payload)
    newer.write_bytes(payload)
    os.utime(older, ns=(1_000_000_000, 1_000_000_000))
    os.utime(newer, ns=(2_000_000_000, 2_000_000_000))

    engine = sieve.Sieve(mode="exact", dup_dir=str(dup), no_cache=True, hash_workers=1)
    engine.walk(str(src))

    assert engine.dup_count == 1
    moved = engine.results["duplicates_moved"][0]
    assert moved["kept"] == str(older.resolve())
    assert moved["source"] == str(newer.resolve())
    assert not newer.exists()
    assert os.path.exists(moved["destination"])


def test_walk_keeps_same_size_non_duplicates(tmp_path):
    src = tmp_path / "src"
    dup = tmp_path / "dups"
    src.mkdir()
    dup.mkdir()

    left = src / "left.bin"
    right = src / "right.bin"
    left.write_bytes(b"A" * 4096)
    right.write_bytes(b"B" * 4096)

    engine = sieve.Sieve(mode="exact", dup_dir=str(dup), no_cache=True, hash_workers=1)
    engine.walk(str(src))

    assert engine.dup_count == 0
    assert left.exists()
    assert right.exists()


def test_walk_many_dedupes_across_roots(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    dup = tmp_path / "dups"
    root_a.mkdir()
    root_b.mkdir()
    dup.mkdir()

    left = root_a / "track.mp3"
    right = root_b / "track-copy.mp3"
    left.write_bytes(b"audio-same" * 1024)
    right.write_bytes(b"audio-same" * 1024)
    os.utime(left, ns=(1_000_000_000, 1_000_000_000))
    os.utime(right, ns=(2_000_000_000, 2_000_000_000))

    engine = sieve.Sieve(mode="exact", dup_dir=str(dup), no_cache=True, hash_workers=1)
    engine.walk_many([str(root_a), str(root_b)])

    assert engine.dup_count == 1
    moved = engine.results["duplicates_moved"][0]
    assert moved["kept"] == str(left.resolve())
    assert moved["source"] == str(right.resolve())


def test_repeated_run_uses_cache_hits(tmp_path):
    src = tmp_path / "src"
    dup = tmp_path / "dups"
    cache_db = tmp_path / "cache.sqlite"
    src.mkdir()
    dup.mkdir()

    left = src / "left.bin"
    right = src / "right.bin"
    left.write_bytes((b"A" * 1000) + (b"C" * 1000))
    right.write_bytes((b"B" * 1000) + (b"D" * 1000))

    engine = sieve.Sieve(
        mode="exact",
        dup_dir=str(dup),
        cache_db=str(cache_db),
        no_cache=False,
        hash_workers=1,
    )
    engine.walk(str(src))
    first_ratio = engine.results["stats"]["cache_hit_ratio"]

    engine.walk(str(src))
    second_ratio = engine.results["stats"]["cache_hit_ratio"]

    assert first_ratio <= second_ratio
    assert second_ratio >= 0.90


def test_sieve_uses_config_values_and_cli_overrides(tmp_path):
    config_path = tmp_path / "sieve.conf"
    config_path.write_text(
        "\n".join(
            [
                "[global]",
                f"dup_dir:{tmp_path / 'config-dups'}",
                "mode:exact",
                f"cache_db:{tmp_path / 'cache-from-config.sqlite'}",
                "hash_workers:3",
                "media_workers:2",
                "[media]",
                "enabled:true",
                "image_hamming_threshold:7",
                "video_hamming_threshold:31",
                "video_frame_hamming_threshold:11",
                "duration_bucket_seconds:4",
            ]
        ),
        encoding="utf-8",
    )

    from_config = sieve.Sieve(config_path=str(config_path))
    assert from_config.mode == "exact"
    assert from_config.hash_workers == 3
    assert from_config.media_workers == 2
    assert from_config.image_hamming_threshold == 7
    assert from_config.video_hamming_threshold == 31
    assert from_config.video_frame_hamming_threshold == 11
    assert from_config.duration_bucket_seconds == 4

    overridden = sieve.Sieve(
        config_path=str(config_path),
        mode="media",
        hash_workers=5,
        media_workers=4,
    )
    assert overridden.mode == "media"
    assert overridden.hash_workers == 5
    assert overridden.media_workers == 4


def test_media_mode_logs_fallback_when_tools_missing(tmp_path, caplog):
    src = tmp_path / "src"
    dup = tmp_path / "dups"
    src.mkdir()
    dup.mkdir()
    (src / "frame.jpg").write_bytes(b"jpeg-like-bytes")

    engine = sieve.Sieve(
        mode="media",
        dup_dir=str(dup),
        no_cache=True,
        ffmpeg_path="definitely-missing-ffmpeg",
        ffprobe_path="definitely-missing-ffprobe",
    )
    with caplog.at_level("WARNING"):
        engine.walk(str(src))

    assert engine.results["similar_media_candidates"] == []
    assert any("skipping perceptual media stage" in rec.message for rec in caplog.records)


def test_sieve_rejects_invalid_mode(tmp_path):
    with pytest.raises(ValueError, match="Invalid mode"):
        sieve.Sieve(mode="invalid-mode", dup_dir=str(tmp_path / "dups"))
