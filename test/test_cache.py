import os

from filesieve.cache import SignatureCache


def test_cache_invalidation_by_mtime_and_size(tmp_path):
    db_path = tmp_path / "cache.sqlite"
    cache = SignatureCache(str(db_path))
    try:
        file_path = os.path.abspath(str(tmp_path / "media.bin"))
        cache.upsert(
            path=file_path,
            size=100,
            mtime_ns=111,
            dev=1,
            ino=2,
            quick_hash="quick",
            full_hash="full",
            media_sig='{"kind":"image","hash":1}',
            media_meta='{"width":100,"height":100,"duration":0.0}',
            last_seen_run="run-a",
        )
        cache.commit()

        record = cache.get(
            path=file_path,
            size=100,
            mtime_ns=111,
            dev=1,
            ino=2,
        )
        assert record is not None
        assert record.quick_hash == "quick"
        assert record.full_hash == "full"

        assert (
            cache.get(
                path=file_path,
                size=100,
                mtime_ns=222,
                dev=1,
                ino=2,
            )
            is None
        )
        assert (
            cache.get(
                path=file_path,
                size=101,
                mtime_ns=111,
                dev=1,
                ino=2,
            )
            is None
        )
    finally:
        cache.close()
