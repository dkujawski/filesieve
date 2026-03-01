"""SQLite-backed cache for exact and media file signatures."""

from __future__ import annotations

from dataclasses import dataclass
import os
import sqlite3


@dataclass(frozen=True)
class CacheRecord:
    """Cached signatures for a single file path and stat identity."""

    quick_hash: str | None
    full_hash: str | None
    media_sig: str | None
    media_meta: str | None


class SignatureCache:
    """Persist and reuse file signatures across runs."""

    def __init__(self, db_path: str) -> None:
        self.db_path = os.path.abspath(db_path)
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._ensure_schema()

    def close(self) -> None:
        self._conn.close()

    def _ensure_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signatures (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                dev INTEGER NOT NULL,
                ino INTEGER NOT NULL,
                quick_hash TEXT,
                full_hash TEXT,
                media_sig TEXT,
                media_meta TEXT,
                last_seen_run TEXT NOT NULL
            );
            """
        )
        self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_signatures_seen
            ON signatures(last_seen_run);
            """
        )
        self._conn.commit()

    def get(
        self,
        *,
        path: str,
        size: int,
        mtime_ns: int,
        dev: int,
        ino: int,
    ) -> CacheRecord | None:
        row = self._conn.execute(
            """
            SELECT quick_hash, full_hash, media_sig, media_meta
            FROM signatures
            WHERE path = ?
              AND size = ?
              AND mtime_ns = ?
              AND dev = ?
              AND ino = ?
            """,
            (path, size, mtime_ns, dev, ino),
        ).fetchone()
        if row is None:
            return None
        return CacheRecord(
            quick_hash=row[0],
            full_hash=row[1],
            media_sig=row[2],
            media_meta=row[3],
        )

    def upsert(
        self,
        *,
        path: str,
        size: int,
        mtime_ns: int,
        dev: int,
        ino: int,
        last_seen_run: str,
        quick_hash: str | None = None,
        full_hash: str | None = None,
        media_sig: str | None = None,
        media_meta: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            INSERT INTO signatures (
                path, size, mtime_ns, dev, ino,
                quick_hash, full_hash, media_sig, media_meta, last_seen_run
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                size = excluded.size,
                mtime_ns = excluded.mtime_ns,
                dev = excluded.dev,
                ino = excluded.ino,
                quick_hash = CASE
                    WHEN (
                        signatures.size <> excluded.size OR
                        signatures.mtime_ns <> excluded.mtime_ns OR
                        signatures.dev <> excluded.dev OR
                        signatures.ino <> excluded.ino
                    ) THEN excluded.quick_hash
                    ELSE COALESCE(excluded.quick_hash, signatures.quick_hash)
                END,
                full_hash = CASE
                    WHEN (
                        signatures.size <> excluded.size OR
                        signatures.mtime_ns <> excluded.mtime_ns OR
                        signatures.dev <> excluded.dev OR
                        signatures.ino <> excluded.ino
                    ) THEN excluded.full_hash
                    ELSE COALESCE(excluded.full_hash, signatures.full_hash)
                END,
                media_sig = CASE
                    WHEN (
                        signatures.size <> excluded.size OR
                        signatures.mtime_ns <> excluded.mtime_ns OR
                        signatures.dev <> excluded.dev OR
                        signatures.ino <> excluded.ino
                    ) THEN excluded.media_sig
                    ELSE COALESCE(excluded.media_sig, signatures.media_sig)
                END,
                media_meta = CASE
                    WHEN (
                        signatures.size <> excluded.size OR
                        signatures.mtime_ns <> excluded.mtime_ns OR
                        signatures.dev <> excluded.dev OR
                        signatures.ino <> excluded.ino
                    ) THEN excluded.media_meta
                    ELSE COALESCE(excluded.media_meta, signatures.media_meta)
                END,
                last_seen_run = excluded.last_seen_run
            """,
            (
                path,
                size,
                mtime_ns,
                dev,
                ino,
                quick_hash,
                full_hash,
                media_sig,
                media_meta,
                last_seen_run,
            ),
        )

    def commit(self) -> None:
        self._conn.commit()

    def prune_stale(self, run_id: str) -> None:
        self._conn.execute(
            """
            DELETE FROM signatures
            WHERE last_seen_run <> ?
            """,
            (run_id,),
        )
        self._conn.commit()
