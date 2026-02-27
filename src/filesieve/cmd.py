#!/usr/bin/env python
"""CLI entrypoint for recursively finding duplicate files."""

import argparse
import logging
import os
import sys

from filesieve import sieve

DESCRIPTION = """Recursively walk the base directory moving out any
duplicate files into an alternate directory leaving only unique
files remaining in the base directory tree.
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
            os.mkdir(resolved_path)
        except OSError as e:
            msg = (
                "unable to create alternate directory for duplicate files:"
                f"\n\t{resolved_path}\n{str(e)}"
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
        '-c', '--config', type=is_valid_config,
        help='optional config file path (precedence: CLI args > config file > defaults)'
    )
    parser.add_argument('-a', '--alternate', type=is_createable_dir,
                        help='move all duplicate files into this directory')
    parser.add_argument('base', nargs='+', type=is_valid_dir,
                        help='the base directory tree to search')
    return parser


def main() -> int:
    """Execute the CLI workflow."""
    logging.basicConfig(level=logging.INFO)
    parser = build_parser()
    args = parser.parse_args()

    try:
        s = sieve.Sieve(config_path=args.config, dup_dir=args.alternate)
    except ValueError as exc:
        parser.error(str(exc))

    if not args.base:
        parser.error("at least one base directory must be provided")

    for path in args.base:
        LOGGER.info("Processing base path: %s", path)
        s.walk(path)

    return 0

if __name__ == '__main__':
    sys.exit(main())
    
