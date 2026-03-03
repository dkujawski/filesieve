"""Media organization workflow with CLI+Tkinter UI support."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
import queue
import re
import shutil
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except Exception:  # pragma: no cover - headless/test environments
    tk = None
    filedialog = None
    messagebox = None
    ttk = None

VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".wmv", ".ts", ".webm"}
DEFAULT_STATE_DB = ".filesieve-organizer.sqlite"
DEFAULT_CONFIG_PATH = "config/organize.yaml"


@dataclass(frozen=True)
class OrganizerConfig:
    preset: str = "plex"
    duplicates_dir_name: str = "Duplicates"
    unsorted_dir_name: str = "Unsorted"
    dry_run: bool = True


@dataclass(frozen=True)
class MediaRecord:
    source: str
    title: str
    year: int | None
    season: int | None
    episode: int | None
    resolution_score: int
    size: int
    extension: str
    dedupe_key: str


class OrganizerState:
    def __init__(self, db_path: str) -> None:
        self.db_path = os.path.abspath(db_path)
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_state (
                source_path TEXT PRIMARY KEY,
                source_sha256 TEXT,
                destination_path TEXT,
                destination_sha256 TEXT,
                last_status TEXT,
                updated_at TEXT
            )
            """
        )
        self.conn.commit()

    def get(self, source_path: str) -> tuple[str, str, str] | None:
        row = self.conn.execute(
            "SELECT source_sha256, destination_path, destination_sha256 FROM file_state WHERE source_path=?",
            (source_path,),
        ).fetchone()
        if row is None:
            return None
        return str(row[0] or ""), str(row[1] or ""), str(row[2] or "")

    def upsert(self, source_path: str, source_sha256: str, destination_path: str, destination_sha256: str, status: str) -> None:
        self.conn.execute(
            """
            INSERT INTO file_state(source_path, source_sha256, destination_path, destination_sha256, last_status, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_path) DO UPDATE SET
                source_sha256=excluded.source_sha256,
                destination_path=excluded.destination_path,
                destination_sha256=excluded.destination_sha256,
                last_status=excluded.last_status,
                updated_at=excluded.updated_at
            """,
            (source_path, source_sha256, destination_path, destination_sha256, status, datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def load_yaml_config(path: str | None) -> OrganizerConfig:
    if path is None:
        return OrganizerConfig()
    payload: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            payload[key.strip()] = value.strip().strip("\"'")
    return OrganizerConfig(
        preset=payload.get("preset", "plex"),
        duplicates_dir_name=payload.get("duplicates_dir_name", "Duplicates"),
        unsorted_dir_name=payload.get("unsorted_dir_name", "Unsorted"),
        dry_run=payload.get("dry_run", "true").lower() in {"1", "true", "yes", "on"},
    )


def _sha256(path: str, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _resolution_score(name: str) -> int:
    lowered = name.lower()
    if "2160p" in lowered or "4k" in lowered:
        return 2160
    if "1080p" in lowered:
        return 1080
    if "720p" in lowered:
        return 720
    if "480p" in lowered:
        return 480
    return 0


def _parse_media_name(path: str) -> MediaRecord | None:
    ext = Path(path).suffix.lower()
    if ext not in VIDEO_EXTENSIONS:
        return None
    base = Path(path).stem
    show_match = re.search(r"(?P<title>.+?)[ ._-]+S(?P<season>\d{2})E(?P<episode>\d{2})", base, re.IGNORECASE)
    if show_match:
        title = show_match.group("title").replace(".", " ").replace("_", " ").strip()
        season = int(show_match.group("season"))
        episode = int(show_match.group("episode"))
        dedupe_key = f"show:{title.lower()}|s{season:02d}e{episode:02d}"
        size = os.path.getsize(path)
        return MediaRecord(
            source=os.path.abspath(path),
            title=title,
            year=None,
            season=season,
            episode=episode,
            resolution_score=_resolution_score(base),
            size=size,
            extension=ext,
            dedupe_key=dedupe_key,
        )

    movie_match = re.search(r"(?P<title>.+?)[ ._-]*\((?P<year>\d{4})\)", base)
    if movie_match:
        title = movie_match.group("title").replace(".", " ").replace("_", " ").strip()
        year = int(movie_match.group("year"))
        dedupe_key = f"movie:{title.lower()}|{year}"
    else:
        cleaned = re.sub(r"[._-]+", " ", base).strip()
        title = cleaned
        year = None
        dedupe_key = f"unknown:{title.lower()}"
    return MediaRecord(
        source=os.path.abspath(path),
        title=title,
        year=year,
        season=None,
        episode=None,
        resolution_score=_resolution_score(base),
        size=os.path.getsize(path),
        extension=ext,
        dedupe_key=dedupe_key,
    )


def _safe_title(value: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "", value)
    return re.sub(r"\s+", " ", cleaned).strip() or "Unknown"


def _plex_destination(record: MediaRecord, target_root: str, unsorted_dir: str) -> str:
    title = _safe_title(record.title)
    if record.season is not None and record.episode is not None:
        season_dir = f"Season {record.season:02d}"
        filename = f"{title} - S{record.season:02d}E{record.episode:02d}{record.extension}"
        return os.path.join(target_root, "TV", title, season_dir, filename)
    if record.year is not None:
        movie_folder = f"{title} ({record.year})"
        filename = f"{title} ({record.year}){record.extension}"
        return os.path.join(target_root, "Movies", movie_folder, filename)
    return os.path.join(target_root, unsorted_dir, f"{title}{record.extension}")


def _versioned_destination(path: str) -> str:
    if not os.path.exists(path):
        return path
    stem = str(Path(path).with_suffix(""))
    ext = Path(path).suffix
    idx = 2
    while True:
        candidate = f"{stem} ({idx}){ext}"
        if not os.path.exists(candidate):
            return candidate
        idx += 1


class MediaOrganizer:
    def __init__(
        self,
        *,
        sources: list[str],
        target_root: str,
        config: OrganizerConfig,
        state_db: str,
        dry_run: bool | None = None,
    ) -> None:
        self.sources = [os.path.abspath(item) for item in sources]
        self.target_root = os.path.abspath(target_root)
        self.config = config
        self.dry_run = config.dry_run if dry_run is None else dry_run
        self.state = OrganizerState(state_db)
        self.pause_event = threading.Event()
        self.stop_requested = False

    def close(self) -> None:
        self.state.close()

    def pause(self) -> None:
        self.pause_event.set()

    def resume(self) -> None:
        self.pause_event.clear()

    def stop(self) -> None:
        self.stop_requested = True

    def _iter_media_files(self) -> list[str]:
        files: list[str] = []
        for source in self.sources:
            for root, _, names in os.walk(source):
                for name in sorted(names):
                    path = os.path.join(root, name)
                    if Path(path).suffix.lower() in VIDEO_EXTENSIONS:
                        files.append(path)
        return files

    def _pick_canonical(self, entries: list[MediaRecord]) -> MediaRecord:
        return sorted(entries, key=lambda item: (item.resolution_score, item.size, -len(item.source)), reverse=True)[0]

    def run(self, progress: Callable[[dict[str, object]], None] | None = None) -> dict[str, object]:
        started = time.time()
        items = [rec for rec in (_parse_media_name(path) for path in self._iter_media_files()) if rec is not None]
        groups: dict[str, list[MediaRecord]] = {}
        for rec in items:
            groups.setdefault(rec.dedupe_key, []).append(rec)

        duplicates_dir = os.path.join(self.target_root, self.config.duplicates_dir_name)
        os.makedirs(duplicates_dir, exist_ok=True)

        operations: list[dict[str, str]] = []
        processed = 0
        moved = 0

        for key in sorted(groups):
            entries = groups[key]
            canonical = self._pick_canonical(entries)
            for entry in sorted(entries, key=lambda rec: rec.source):
                if self.stop_requested:
                    break
                while self.pause_event.is_set() and not self.stop_requested:
                    time.sleep(0.1)

                processed += 1
                source_hash = _sha256(entry.source)

                if entry.source == canonical.source:
                    destination = _plex_destination(entry, self.target_root, self.config.unsorted_dir_name)
                    status = "organized"
                else:
                    destination = os.path.join(duplicates_dir, os.path.basename(entry.source))
                    status = "duplicate"

                destination = _versioned_destination(destination)
                state_record = self.state.get(entry.source)
                if state_record is not None and state_record[0] == source_hash and state_record[1] == destination:
                    if os.path.exists(destination):
                        status = "already-current"
                        if progress is not None:
                            progress(
                                {
                                    "processed": processed,
                                    "total": len(items),
                                    "moved": moved,
                                    "status": status,
                                    "source": entry.source,
                                    "destination": destination,
                                    "eta_seconds": _eta(started, processed, len(items)),
                                    "throughput": _throughput(started, processed),
                                }
                            )
                        continue

                operations.append({"source": entry.source, "destination": destination, "status": status})
                if not self.dry_run:
                    os.makedirs(os.path.dirname(destination), exist_ok=True)
                    _move_with_verify(entry.source, destination)
                    moved += 1
                    destination_hash = _sha256(destination)
                else:
                    destination_hash = source_hash
                self.state.upsert(entry.source, source_hash, destination, destination_hash, status)

                if progress is not None:
                    progress(
                        {
                            "processed": processed,
                            "total": len(items),
                            "moved": moved,
                            "status": status,
                            "source": entry.source,
                            "destination": destination,
                            "eta_seconds": _eta(started, processed, len(items)),
                            "throughput": _throughput(started, processed),
                        }
                    )
            if self.stop_requested:
                break

        return {
            "total": len(items),
            "processed": processed,
            "moved": moved,
            "dry_run": self.dry_run,
            "operations": operations,
            "stopped": self.stop_requested,
        }


def _move_with_verify(source: str, destination: str) -> None:
    if os.path.splitdrive(source)[0].lower() == os.path.splitdrive(destination)[0].lower() and os.name == "nt":
        shutil.move(source, destination)
        return
    if os.name != "nt":
        shutil.move(source, destination)
        return
    shutil.copy2(source, destination)
    if _sha256(source) != _sha256(destination):
        raise RuntimeError(f"copy verification failed for {source}")
    os.remove(source)


def _throughput(started: float, processed: int) -> float:
    elapsed = max(time.time() - started, 0.001)
    return processed / elapsed


def _eta(started: float, processed: int, total: int) -> float:
    if processed <= 0:
        return float(total)
    rate = _throughput(started, processed)
    if rate <= 0:
        return float(total)
    return max(0.0, (total - processed) / rate)


def write_default_yaml(path: str) -> None:
    payload = "\n".join(
        [
            "# filesieve organizer config",
            "preset: plex",
            "duplicates_dir_name: Duplicates",
            "unsorted_dir_name: Unsorted",
            "dry_run: true",
            "",
        ]
    )
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(payload)


class OrganizerUI:
    def __init__(self, organizer_factory: Callable[[list[str], str, bool], MediaOrganizer], default_target: str) -> None:
        if tk is None or ttk is None:
            raise RuntimeError("Tkinter is unavailable in this environment")
        self.organizer_factory = organizer_factory
        self.root = tk.Tk()
        self.root.title("filesieve organizer")
        self.sources: list[str] = []
        self.worker: threading.Thread | None = None
        self.organizer: MediaOrganizer | None = None
        self.queue: queue.Queue[dict[str, object]] = queue.Queue()

        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")

        self.source_list = tk.Listbox(frame, width=70, height=6)
        self.source_list.grid(row=0, column=0, columnspan=3, sticky="ew")

        ttk.Button(frame, text="Add Source", command=self._add_source).grid(row=1, column=0, sticky="w")
        ttk.Button(frame, text="Remove Selected", command=self._remove_source).grid(row=1, column=1, sticky="w")

        ttk.Label(frame, text="Target Root").grid(row=2, column=0, sticky="w")
        self.target_var = tk.StringVar(value=default_target)
        ttk.Entry(frame, textvariable=self.target_var, width=70).grid(row=3, column=0, columnspan=2, sticky="ew")
        ttk.Button(frame, text="Browse", command=self._pick_target).grid(row=3, column=2, sticky="w")

        self.dry_run_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(frame, text="Dry run", variable=self.dry_run_var).grid(row=4, column=0, sticky="w")

        ttk.Button(frame, text="Start", command=self._start).grid(row=5, column=0, sticky="w")
        ttk.Button(frame, text="Pause", command=self._pause).grid(row=5, column=1, sticky="w")
        ttk.Button(frame, text="Continue", command=self._resume).grid(row=5, column=2, sticky="w")
        ttk.Button(frame, text="Stop", command=self._stop).grid(row=6, column=0, sticky="w")

        self.progress_var = tk.StringVar(value="idle")
        ttk.Label(frame, textvariable=self.progress_var).grid(row=7, column=0, columnspan=3, sticky="w")

        self.root.after(200, self._poll_queue)

    def _add_source(self) -> None:
        selected = filedialog.askdirectory()
        if selected:
            self.sources.append(selected)
            self.source_list.insert(tk.END, selected)

    def _remove_source(self) -> None:
        selection = self.source_list.curselection()
        if not selection:
            return
        idx = selection[0]
        self.sources.pop(idx)
        self.source_list.delete(idx)

    def _pick_target(self) -> None:
        selected = filedialog.askdirectory()
        if selected:
            self.target_var.set(selected)

    def _start(self) -> None:
        if not self.sources:
            messagebox.showerror("filesieve", "Select at least one source directory")
            return
        if self.worker is not None and self.worker.is_alive():
            return
        self.organizer = self.organizer_factory(list(self.sources), self.target_var.get(), self.dry_run_var.get())

        def _run() -> None:
            assert self.organizer is not None
            result = self.organizer.run(progress=lambda item: self.queue.put(item))
            self.queue.put({"final": result})
            self.organizer.close()

        self.worker = threading.Thread(target=_run, daemon=True)
        self.worker.start()

    def _pause(self) -> None:
        if self.organizer is not None:
            self.organizer.pause()

    def _resume(self) -> None:
        if self.organizer is not None:
            self.organizer.resume()

    def _stop(self) -> None:
        if self.organizer is not None:
            self.organizer.stop()

    def _poll_queue(self) -> None:
        try:
            while True:
                event = self.queue.get_nowait()
                if "final" in event:
                    final = event["final"]
                    self.progress_var.set(
                        f"done: processed={final['processed']} moved={final['moved']} dry_run={final['dry_run']}"
                    )
                else:
                    self.progress_var.set(
                        "processed={processed}/{total} moved={moved} throughput={throughput:.2f} ETA={eta_seconds:.1f}s {status}".format(
                            **event
                        )
                    )
        except queue.Empty:
            pass
        self.root.after(200, self._poll_queue)

    def run(self) -> None:
        self.root.mainloop()
