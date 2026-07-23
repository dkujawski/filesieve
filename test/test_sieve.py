import os

import pytest

from filesieve import media, sieve


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


@pytest.mark.parametrize("overlap", ["same-root", "parent-child"])
def test_walk_many_does_not_treat_overlapping_roots_as_duplicates(tmp_path, overlap):
    root = tmp_path / "root"
    nested = root / "nested"
    dup = tmp_path / "dups"
    nested.mkdir(parents=True)
    dup.mkdir()

    only_file = nested / "only.bin"
    only_file.write_bytes(b"unique-content")

    base_dirs = [str(root), str(root)] if overlap == "same-root" else [str(root), str(nested)]
    engine = sieve.Sieve(mode="exact", dup_dir=str(dup), no_cache=True, hash_workers=1)
    engine.walk_many(base_dirs)

    assert engine.dup_count == 0
    assert only_file.exists()
    assert list(dup.rglob("only.bin")) == []


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


def _fake_scaled_media_result(small_path, large_path):
    return media.MediaPipelineResult(
        similar_media_candidates=[
            {
                "cluster_id": "media-1",
                "paths": [str(small_path), str(large_path)],
                "score_summary": {
                    "kind": media.IMAGE_KIND,
                    "pairs": 1,
                    "min": 0,
                    "max": 0,
                },
            }
        ],
        media_metadata={
            str(small_path): {"width": 512, "height": 512, "duration": 0.0},
            str(large_path): {"width": 1024, "height": 1024, "duration": 0.0},
        },
        cache_hits=0,
        cache_misses=0,
        tools_available=True,
    )


def test_similar_media_remains_report_only_by_default(tmp_path, monkeypatch):
    source = tmp_path / "source"
    duplicate_dir = tmp_path / "duplicates"
    source.mkdir()
    small = source / "small.png"
    large = source / "large.png"
    small.write_bytes(b"small-file")
    large.write_bytes(b"large-file")

    monkeypatch.setattr(
        sieve,
        "run_media_pipeline",
        lambda *args, **kwargs: _fake_scaled_media_result(small, large),
    )

    engine = sieve.Sieve(
        mode="media",
        dup_dir=str(duplicate_dir),
        no_cache=True,
        hash_workers=1,
        media_workers=1,
    )
    engine.walk(str(source))

    assert engine.dup_count == 0
    assert small.exists()
    assert large.exists()


def test_move_similar_keeps_highest_resolution(tmp_path, monkeypatch):
    source = tmp_path / "source"
    duplicate_dir = tmp_path / "duplicates"
    source.mkdir()
    small = source / "small.png"
    large = source / "large.png"
    small.write_bytes(b"s" * 200)
    large.write_bytes(b"l" * 100)
    os.utime(small, ns=(1_000_000_000, 1_000_000_000))
    os.utime(large, ns=(2_000_000_000, 2_000_000_000))

    monkeypatch.setattr(
        sieve,
        "run_media_pipeline",
        lambda *args, **kwargs: _fake_scaled_media_result(small, large),
    )

    engine = sieve.Sieve(
        mode="media",
        move_similar=True,
        dup_dir=str(duplicate_dir),
        no_cache=True,
        hash_workers=1,
        media_workers=1,
    )
    engine.walk(str(source))

    assert engine.dup_count == 1
    assert large.exists()
    assert not small.exists()
    moved = engine.results["duplicates_moved"][0]
    assert moved["source"] == str(small)
    assert moved["kept"] == str(large)
    assert moved["reason"] == "perceptual-similar"
    assert os.path.exists(moved["destination"])

    cluster = engine.results["similar_media_candidates"][0]
    assert cluster["kept"] == str(large)
    assert cluster["moved"] == [moved]


def test_sieve_rejects_invalid_mode(tmp_path):
    with pytest.raises(ValueError, match="Invalid mode"):
        sieve.Sieve(mode="invalid-mode", dup_dir=str(tmp_path / "dups"))


def test_sieve_rejects_move_similar_in_exact_mode(tmp_path):
    with pytest.raises(ValueError, match="--move-similar requires --mode media"):
        sieve.Sieve(
            mode="exact",
            move_similar=True,
            dup_dir=str(tmp_path / "dups"),
        )
