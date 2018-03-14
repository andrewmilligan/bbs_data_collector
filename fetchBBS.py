#!/usr/bin/env python

import bbs
import argparse
import os
import sys

program_name = "fetchBBS.py"
program_description = "a script to fetch BBS data and store them in a DB"
argparser = argparse.ArgumentParser(prog=program_name,
    description=program_description)

argparser.add_argument('--working-directory', '-w',
    default=os.getcwd(),
    help="working directory to store stuff in")

args = argparser.parse_args()

if not os.path.isdir(args.working_directory):
  err = "ERROR: working directory '{}' does not exist"
  print(err.format(args.working_directory))
  sys.exit(1)

bbs.BBS(wd=args.working_directory).fetchAndCreateAll()
