import bbs
import argparse
import os

program_name = "fetchBBS.py"
program_description = "a script to fetch BBS data and store them in a DB"
argparser = argparse.ArgumentParser(prog=program_name,
    description=program_description)

argparser.add_argument('--working-directory', '-w',
    default=os.getcwd(),
    help="working directory to store stuff in")

args = argparser.parse_args()

bbs.BBS(wd=args.working_directory).fetchAndCreateAll()
