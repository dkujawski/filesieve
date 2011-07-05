#!/usr/bin/env python
'''
Created on 5/07/2011

@author: dave
'''
import argparse

DESC = """Recursively walk the base directory moving out any
duplicate files into an alternate directory leaving only unique
files remaining in the base directory tree.
"""

def build_parser():
    parser = argparse.ArgumentParser(description=DESC)
    
    return parser

if __name__ == '__main__':
    bp = build_parser()
    bp.parse_args()
    
    