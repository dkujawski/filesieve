#!/usr/bin/env python
"""CLI entrypoint for recursively finding duplicate files."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from filesieve import sieve


DESCRIPTION = """Recursively walk one or more base directories, moving exact
duplicate files into an alternate directory while optionally generating
perceptual similarity reports for media files.
"""

LOGGER = logging.getLogger(__name__)


def is_valid_dir(path_str: str) -> str:
    """Validate that ``path_str`` exists and is a directory."""
    resolved_path = os.path.abspath(path_str)
    if os.path.exists(resolved_path) and os.path.isdir(resolved_path):
        return resolved_path
    msg = f"path is not a directory or does not exist:\n\t{resolved_path}"
    raise argparse.ArgumentTypeError(msg)


def is_createable_dir(path_str: str) -> str:
    """Validate ``path_str`` as an existing directory or create it."""
    resolved_path = os.path.abspath(path_str)
    if not (os.path.exists(resolved_path) and os.path.isdir(resolved_path)):
        try:
            os.makedirs(resolved_path, exist_ok=True)
        except OSError as exc:
            msg = (
                "unable to create alternate directory for duplicate files:"
                f"\n\t{resolved_path}\n{str(exc)}"
            )
            raise argparse.ArgumentTypeError(msg)
    return resolved_path


def is_valid_config(path_str: str) -> str:
    """Validate config path exists and is a file."""
    resolved_path = os.path.abspath(path_str)
    if os.path.exists(resolved_path) and os.path.isfile(resolved_path):
        return resolved_path
    msg = f"config path is not a file or does not exist:\n\t{resolved_path}"
    raise argparse.ArgumentTypeError(msg)


def build_parser() -> argparse.ArgumentParser:
    """Build and return the command line parser."""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "-c",
        "--config",
        type=is_valid_config,
        help="optional config file path (precedence: CLI args > config file > defaults)",
    )
    parser.add_argument(
        "-a",
        "--alternate",
        type=is_createable_dir,
        help="move exact duplicate files into this directory",
    )
    parser.add_argument(
        "--mode",
        choices=("exact", "media"),
        help="duplicate detection mode (exact or media)",
    )
    parser.add_argument(
        "--cache",
        help="sqlite path for persistent signature cache",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="disable cache usage for this run",
    )
    parser.add_argument(
        "--hash-workers",
        type=int,
        help="number of worker threads for exact hashing",
    )
    parser.add_argument(
        "--media-workers",
        type=int,
        help="number of worker threads for media perceptual signatures",
    )
    parser.add_argument(
        "--ffmpeg",
        help="path or executable name for ffmpeg",
    )
    parser.add_argument(
        "--ffprobe",
        help="path or executable name for ffprobe",
    )
    parser.add_argument(
        "--report-similar",
        help="write perceptual media similarity clusters to this JSON file",
    )
    parser.add_argument("base", nargs="+", type=is_valid_dir, help="base directory tree(s)")
    return parser


def main() -> int:
    """Execute the CLI workflow."""
    logging.basicConfig(level=logging.INFO)
    parser = build_parser()
    args = parser.parse_args()

    try:
        engine = sieve.Sieve(
            config_path=args.config,
            dup_dir=args.alternate,
            mode=args.mode,
            cache_db=args.cache,
            no_cache=args.no_cache,
            hash_workers=args.hash_workers,
            media_workers=args.media_workers,
            ffmpeg_path=args.ffmpeg,
            ffprobe_path=args.ffprobe,
        )
    except ValueError as exc:
        parser.error(str(exc))

    LOGGER.info("Processing %d base path(s)", len(args.base))
    engine.walk_many(args.base)

    if args.report_similar:
        report_path = os.path.abspath(args.report_similar)
        parent = os.path.dirname(report_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        payload = engine.results.get("similar_media_candidates", [])
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        LOGGER.info("Wrote %d similar-media clusters to %s", len(payload), report_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
