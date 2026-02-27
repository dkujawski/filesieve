"""Core duplicate-file detection primitives for FileSieve."""

from collections import defaultdict
import hashlib
import logging
import os
import shutil
from configparser import ConfigParser
from typing import DefaultDict


DEFAULT_READ_SIZE = 1024
DEFAULT_DUP_DIR = "/tmp/sieve/dups"

LOGGER = logging.getLogger(__name__)


class Sieve:
    """Initialize config state and identify duplicate files.

    Configuration precedence order is:

    1. explicit constructor values (CLI args)
    2. optional config file values
    3. in-code defaults

    Example ::

        base_dir = "/vol/musix/Music"
        s = Sieve()
        file_dict = s.walk(base_dir)

    """

    def __init__(
        self,
        *,
        config_path: str | None = None,
        read_size: int | None = None,
        dup_dir: str | None = None,
    ) -> None:
        config = self.__get_config(config_path)

        merged_read_size = DEFAULT_READ_SIZE
        merged_dup_dir = DEFAULT_DUP_DIR
        if config is not None:
            merged_read_size = int(
                config.get("global", "read_size", fallback=str(merged_read_size))
            )
            merged_dup_dir = config.get("global", "dup_dir", fallback=merged_dup_dir)
        if read_size is not None:
            merged_read_size = read_size
        if dup_dir is not None:
            merged_dup_dir = dup_dir

        self.read_size = self.__validate_read_size(merged_read_size)
        self.dup_dir = self.__validate_dup_dir(merged_dup_dir)
        # dup trackers
        self.dup_keys = set()
        self.results: dict[str, list[dict[str, str]]] = {"duplicates_moved": []}
        # main data bucket
        self.data: DefaultDict[str, list[str]] = defaultdict(list)

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

    def __validate_read_size(self, read_size: int) -> int:
        """Validate read_size is a positive integer."""
        if not isinstance(read_size, int):
            raise ValueError("Invalid config value for read_size: must be an integer")
        if read_size <= 0:
            raise ValueError("Invalid config value for read_size: must be greater than 0")
        return read_size

    def __validate_dup_dir(self, dup_dir: str) -> str:
        """Validate dup_dir exists (or can be created) and is writable."""
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

    @property
    def dup_count(self) -> int:
        """Return number of duplicates found since object init."""
        return len(self.dup_keys)

    def walk(self, base_dir: str) -> dict[str, list[str]]:
        """Recursively walk ``base_dir`` collecting file hash metadata.

        The first-seen file for a given content hash is kept in place. Any
        later file with the same hash is treated as a duplicate and moved into
        ``dup_dir``.
        """
        if not isinstance(base_dir, str):
            raise TypeError("base_dir must be a string type")
        if not os.path.exists(base_dir):
            LOGGER.error("Base directory tree does not exist: %s", base_dir)
            return dict(self.data)
        # walk the base_dir, we don't care about directory names.
        for root, _, files in os.walk(base_dir):
            for fn in sorted(files):
                # build the full pile path
                fp = os.path.join(root, fn)
                # process the file data to get the hash key
                key = process_file(fp, self.read_size)
                # check to see if we have seen this hash before
                if key in self.data:
                    self.dup_keys.add(key)
                    try:
                        destination = clean_dup(fp, self.dup_dir)
                    except OSError:
                        LOGGER.exception("Unable to move duplicate file: %s", fp)
                    else:
                        self.results["duplicates_moved"].append(
                            {"source": fp, "destination": destination}
                        )
                    continue
                # add the key and the data dict
                self.data[key].append(fp)
        return dict(self.data)


def process_file(file_path: str, read_size: int) -> str:
    """Generate a hash key from file contents."""
    double_read_size = 2 * read_size
    if os.stat(file_path).st_size > double_read_size:
        # For large files, read just the first and last chunks.
        with open(file_path, "rb") as fh:
            first = fh.read(read_size)
            neg_read_size = -1 * read_size
            fh.seek(neg_read_size, os.SEEK_END)
            last = fh.read(read_size)
        chunk = first + last
    else:
        # For small files, hash everything.
        with open(file_path, "rb") as fh:
            chunk = fh.read()
    # build a hash key based on the file data
    return get_hash_key(chunk)


def get_hash_key(data: bytes) -> str:
    """Generate an MD5 key from byte data."""
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("data must be bytes")
    md5 = hashlib.md5()
    md5.update(data)
    key = md5.hexdigest()
    return key


def clean_dup(dup_file: str, dup_dir: str) -> str:
    """Move ``dup_file`` into a mirrored directory rooted in ``dup_dir``."""
    dest = os.path.join(dup_dir, dup_file.lstrip("/"))
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    shutil.move(dup_file, dest)
    return dest
