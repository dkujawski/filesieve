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
CONFIG_FILE = "config/sieve.conf"

LOGGER = logging.getLogger(__name__)


class Sieve:
    """Initialize config state and identify duplicate files.

    The main sieve.conf file can be in one of two places. First the
    FILESIEVE_ROOT environment variable is checked. If this is not found,
    the config file is assumed to live in the config dir three dirs up from
    this module.

    Example ::

        base_dir = "/vol/musix/Music"
        s = Sieve()
        file_dict = s.walk(base_dir)

    """

    def __init__(self) -> None:
        # set defaults from config
        config = self.__get_config()
        self.read_size = int(
            config.get("global", "read_size", fallback=str(DEFAULT_READ_SIZE))
        )
        self.dup_dir = config.get("global", "dup_dir", fallback=DEFAULT_DUP_DIR)
        # dup trackers
        self.dup_keys = set()
        self.results: dict[str, list[dict[str, str]]] = {"duplicates_moved": []}
        # main data bucket
        self.data: DefaultDict[str, list[str]] = defaultdict(list)

    def __get_config(self) -> ConfigParser:
        """Find and load the config file."""
        # where is the config file?
        env_path = os.environ.get("FILESIEVE_ROOT")
        if not env_path:
            rel_path = os.path.join("../../../")
            cur_path = os.path.abspath(__file__)
            env_path = os.path.abspath(os.path.join(cur_path, rel_path))
        # get config settings
        config_path = os.path.join(env_path, CONFIG_FILE)
        if not os.path.exists(config_path):
            LOGGER.warning("Unable to locate config file, using defaults.")
        config = ConfigParser()
        config.read(config_path)
        return config

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
            for fn in files:
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
