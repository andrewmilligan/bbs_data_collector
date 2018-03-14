Breeding Bird Survey (BBS) Data Collector
=========================================

This mainly provides an interface class to fetch BBS data and store it locally
in a SQLite database. The `BBS` class (defined in `bbs.py`) provides member
functions to `wget` the fifty stop data files and the relevant metadata files
from the BBS FTP server; it also provides member functions to parse all of
these various files and store them in a SQLite database.


## Directory Structure

When downloading the data files, the `BBS` class will create the following
directory tree starting at the specified working directory `<cwd>`:

* `<cwd>/`
  * `data/`
    * `meta/`
    * `fifty_stops/`

The `meta` directory contains the downloaded metadata files, which include
information about what various variable codes mean. The `fifty_stops` directory
contains the actual data observations that make up the BBS fifty stop dataset.


## Usage

The included `fetchBBS.py` script provides a very simple command-line interface
for the `BBS` class. You can run it, setting the working directory from the
command line with the `--working-directory` (`-w`) flag, determining which
local directory to store the data files and the database in.


## Requirements

The only non-standard requirements are the [Python `wget` module][1] and
[SQLite3][2].



[1]: https://pypi.python.org/pypi/wget
[2]: https://docs.python.org/2/library/sqlite3.html
