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


def build_parser() -> argparse.ArgumentParser:
    """Build and return the command line parser."""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
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

    s = sieve.Sieve()
    if args.alternate:
        s.dup_dir = args.alternate

    if not args.base:
        parser.error("at least one base directory must be provided")

    for path in args.base:
        LOGGER.info("Processing base path: %s", path)
        s.walk(path)

    return 0

if __name__ == '__main__':
    sys.exit(main())
    
