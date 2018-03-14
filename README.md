Breeding Bird Survey (BBS) Data Collector
=========================================

This mainly provides an interface class to fetch BBS data and store it locally
in a SQLite database. The `BBS` class (defined in `bbs.py`) provides member
functions to `wget` the fifty stop data files and the relevant metadata files
from the BBS FTP server; it also provides member functions to parse all of
these various files and store them in a SQLite database.
