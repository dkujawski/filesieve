import hashlib
import os
import shutil

import pytest

from filesieve import sieve


@pytest.fixture
def test_data_dir() -> str:
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")


@pytest.fixture
def sieve_tree(tmp_path, test_data_dir):
    src = tmp_path / "src"
    dup = tmp_path / "dup"
    shutil.copytree(test_data_dir, src)
    dup.mkdir()
    return src, dup


def test_walk_moves_only_later_duplicates(sieve_tree):
    src, dup = sieve_tree
    s = sieve.Sieve()
    s.dup_dir = str(dup)

    found = s.walk(str(src))

    assert found == {
        "787ada88e6c442bb3ec6b30c97b9126c": [str(src / "big_diff.log")],
        "c86eaa9d51d51dfe1a6a404739f62303": [str(src / "small_diff.log")],
        "5819b7a15d098be2c28f04e6edfb7515": [str(src / "big_copy.log")],
        "ca77696740831b2ac340f71140e641cb": [str(src / "small_copy.log")],
    }
    assert s.dup_count == 2

    moved_sources = {entry["source"] for entry in s.results["duplicates_moved"]}
    assert moved_sources == {str(src / "big_orig.log"), str(src / "small_orig.log")}

    assert (src / "big_copy.log").exists()
    assert (src / "small_copy.log").exists()
    assert not (src / "big_orig.log").exists()
    assert not (src / "small_orig.log").exists()

    assert (dup / str(src / "big_orig.log").lstrip("/")).exists()
    assert (dup / str(src / "small_orig.log").lstrip("/")).exists()


def test_clean_dup_moves_file_into_mirrored_path(sieve_tree):
    src, dup = sieve_tree
    dup_file = src / "small_copy.log"

    sieve.clean_dup(str(dup_file), str(dup))

    expected = dup / str(dup_file).lstrip("/")
    assert expected.exists()
    assert not dup_file.exists()


def test_clean_dup_keeps_distinct_duplicate_paths(tmp_path):
    dup = tmp_path / "dup"
    dup.mkdir()

    left = tmp_path / "a" / "dup.log"
    right = tmp_path / "b" / "dup.log"
    left.parent.mkdir()
    right.parent.mkdir()
    left.write_text("first")
    right.write_text("second")

    sieve.clean_dup(str(left), str(dup))
    sieve.clean_dup(str(right), str(dup))

    left_dest = dup / str(left).lstrip("/")
    right_dest = dup / str(right).lstrip("/")
    assert left_dest.exists()
    assert right_dest.exists()
    assert left_dest.read_text() == "first"
    assert right_dest.read_text() == "second"


def test_get_hash_key():
    expected = "e4578cd35d06171139bad5b66adca0fc"
    data = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "small_orig.log")
    with open(data, "rb") as fh:
        found = sieve.get_hash_key(fh.read())
    assert expected == found


def test_get_hash_key_is_binary_safe():
    binary_data = b"\x00\xff\x00\x80filesieve\n\x10"
    expected = hashlib.md5(binary_data).hexdigest()

    assert sieve.get_hash_key(binary_data) == expected
    assert sieve.get_hash_key(bytearray(binary_data)) == expected


def test_get_hash_key_rejects_non_binary_input():
    with pytest.raises(TypeError):
        sieve.get_hash_key("not-bytes")


def test_walk_logs_and_continues_on_move_failure(sieve_tree, monkeypatch, caplog):
    src, dup = sieve_tree
    s = sieve.Sieve()
    s.dup_dir = str(dup)

    original_clean_dup = sieve.clean_dup

    def fail_clean_dup(path, dup_dir):
        if path.endswith("small_orig.log"):
            raise OSError("boom")
        return original_clean_dup(path, dup_dir)

    monkeypatch.setattr(sieve, "clean_dup", fail_clean_dup)

    with caplog.at_level("ERROR"):
        found = s.walk(str(src))

    assert found["ca77696740831b2ac340f71140e641cb"] == [str(src / "small_copy.log")]
    assert (src / "small_orig.log").exists()
    assert any("Unable to move duplicate file" in rec.message for rec in caplog.records)

    moved_sources = {entry["source"] for entry in s.results["duplicates_moved"]}
    assert str(src / "big_orig.log") in moved_sources
    assert str(src / "small_orig.log") not in moved_sources
