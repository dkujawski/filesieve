import hashlib
import os

import pytest

from filesieve import exact


def _meta(path: str) -> exact.ExactFileMeta:
    stat = os.stat(path)
    return exact.ExactFileMeta(
        path=path,
        size=stat.st_size,
        mtime_ns=stat.st_mtime_ns,
        dev=stat.st_dev,
        ino=stat.st_ino,
    )


def test_quick_hash_clamps_offsets_for_small_files(tmp_path):
    file_path = tmp_path / "small.bin"
    payload = b"filesieve-small-payload"
    file_path.write_bytes(payload)

    digest, bytes_read = exact.quick_hash(str(file_path), size=len(payload), sample_size=64)

    assert digest == hashlib.blake2b(payload, digest_size=16).hexdigest()
    assert bytes_read == len(payload)


def test_full_hash_is_deterministic(tmp_path):
    payload = (b"1234567890abcdef" * 1024) + b"tail"
    left = tmp_path / "left.bin"
    right = tmp_path / "right.bin"
    left.write_bytes(payload)
    right.write_bytes(payload)

    left_hash, left_bytes = exact.full_hash(str(left))
    right_hash, right_bytes = exact.full_hash(str(right))

    assert left_hash == right_hash
    assert left_bytes == len(payload)
    assert right_bytes == len(payload)


def test_byte_compare_blocks_forced_collision(tmp_path, monkeypatch, caplog):
    src = tmp_path / "src"
    dup = tmp_path / "dup"
    src.mkdir()
    dup.mkdir()

    older = src / "older.bin"
    newer = src / "newer.bin"
    older.write_bytes(b"A" * 256)
    newer.write_bytes(b"B" * 256)

    os.utime(older, ns=(1_000_000_000, 1_000_000_000))
    os.utime(newer, ns=(2_000_000_000, 2_000_000_000))

    monkeypatch.setattr(exact, "quick_hash", lambda path, size: ("same-quick", 0))
    monkeypatch.setattr(exact, "full_hash", lambda path: ("same-full", 0))

    with caplog.at_level("WARNING"):
        result = exact.run_exact_pipeline(
            [_meta(str(older)), _meta(str(newer))],
            dup_dir=str(dup),
            hash_workers=1,
            cache=None,
            run_id="run-1",
        )

    assert result.duplicates_moved == []
    assert result.bytes_read_verify > 0
    assert older.exists()
    assert newer.exists()
    assert any("Hash collision anomaly detected" in rec.message for rec in caplog.records)


def test_clean_dup_preserves_distinct_source_paths(tmp_path):
    dup = tmp_path / "dups"
    dup.mkdir()

    left = tmp_path / "left" / "dup.log"
    right = tmp_path / "right" / "dup.log"
    left.parent.mkdir()
    right.parent.mkdir()
    left.write_text("left", encoding="utf-8")
    right.write_text("right", encoding="utf-8")

    left_dest = exact.clean_dup(str(left), str(dup))
    right_dest = exact.clean_dup(str(right), str(dup))

    assert os.path.exists(left_dest)
    assert os.path.exists(right_dest)
    assert left_dest != right_dest
    assert not left.exists()
    assert not right.exists()
