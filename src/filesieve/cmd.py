#!/usr/bin/env python
'''
Created on 5/07/2011

@author: dave
'''
import argparse
import os
import sys

from filesieve import sieve

DESC = """Recursively walk the base directory moving out any
duplicate files into an alternate directory leaving only unique
files remaining in the base directory tree.
"""

def is_valid_dir(path_str):
    """ check to see if the path_str exists on the file system and is a
    directory, if not raise an error.
    """
    resolved_path = os.path.abspath(path_str)
    if os.path.exists(resolved_path) and os.path.isdir(resolved_path):
        return resolved_path
    msg = "path is not a directory or does not exist:\n\t%s" % resolved_path
    raise argparse.ArgumentTypeError(msg)

def is_createable_dir(path_str):
    """ if the path_str already exists and is a directory, return full path.
    if the path_str does not exist, try to create it. if successful, return full
    path.  if the directory cannot be created, raise an error.
    """
    resolved_path = os.path.abspath(path_str)
    if not (os.path.exists(resolved_path) and os.path.isdir(resolved_path)):
        try:
            os.mkdir(resolved_path)
        except Exception as e:
            msg = "unable to create alternate directory for duplicate files:" \
                + "\n\t%s\n%s" % (resolved_path, str(e))
            raise argparse.ArgumentTypeError(msg)
    return resolved_path

def build_parser():
    parser = argparse.ArgumentParser(description=DESC)
    parser.add_argument('-a', '--alternate', type=is_createable_dir,
                        help='move all duplicate files into this directory')
    parser.add_argument('base', nargs='+', type=is_valid_dir, 
                        help='the base directory tree to search')    
    return parser

if __name__ == '__main__':
    bp = build_parser()
    args = bp.parse_args()
    s = sieve.Sieve()
    if args.alternate:
        s.dup_dir = args.alternate
    if not args.base:
        bp.print_help()
        sys.exit(1)
    for path in args.base:
        tmp_data = s.walk(path)
        
    