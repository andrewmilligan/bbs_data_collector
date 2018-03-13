import wget
import zipfile
import os
import glob
import sqlite3
import csv
import table

DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(DIR, 'raw_data')
ZIP_DIR = os.path.join(DATA_DIR, 'zip_files')
CSV_DIR = os.path.join(DATA_DIR, 'csv_files')
META_DIR = os.path.join(DATA_DIR, 'meta')
DB = os.path.join(DIR, "bird_survey_db.sqlite3")

for d in [DATA_DIR, ZIP_DIR, CSV_DIR, META_DIR]:
  if not os.path.isdir(d):
    os.mkdir(d)

url = 'ftp://ftpext.usgs.gov/pub/er/md/laurel/BBS/DataFiles/50-StopData/1997ToPresent_SurveyWide'
files = [ 'Fifty1.zip', 'Fifty2.zip', 'Fifty3.zip', 'Fifty4.zip', 'Fifty5.zip',
    'Fifty6.zip', 'Fifty7.zip', 'Fifty8.zip', 'Fifty9.zip', 'Fifty10.zip' ]

for f in files:
  file_url = '/'.join([url, f])
  file_local = os.path.join(ZIP_DIR, f)

  print("Downloading: {}".format(file_url))
  wget.download(file_url, file_local)
  print("File downloaded.")

  print("Unzipping...")
  zip_ref = zipfile.ZipFile(file_local, 'r')
  zip_ref.extractall(CSV_DIR)
  zip_ref.close()
  print("Done.")

table_name = "fifty_stops"
id_name = "id"
col_names = [
    "RouteDataID", "countrynum", "statenum", "Route", "RPID", "year", "AOU"


import os

def isCleanChar(c):
  return (c >= 'a' and c <= 'z') or \
      (c >= 'A' and c <= 'Z') or \
      (c >= '0' and c <= '9')

def cleanHeader(h):
  h = ''.join([c for c in h if isCleanChar(c)])
  if h == "ORDER":
    h = h.capitalize() + 'X'
  return h


class Table:
  def __init__(self, name, headers, rows):
    self.name = name
    self.headers = headers
    self.rows = rows
    self.cleanHeaders()

  def cleanHeaders(self):
    self.headers = map(cleanHeader, self.headers)
    seen_headers = set()
    for h in self.headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)

def parseLines(lines, table_name, table_ct=0):
  tabs = []

  hrule_index = 0
  for i, line in enumerate(lines):
    if line[:2] == "--":
      hrule_index = i
      break

  if hrule_index == 0:

import os

class Table:
  def __init__(self):
    self.name = ''
    self.headers = []
    self.rows = []


class TableFactory:
  def isCleanChar(c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  def cleanHeader(h):
    h = ''.join([c for c in h if isCleanChar(c)])
    if h == "ORDER":
      h = h.capitalize() + 'X'
    return h

  def cleanHeaders(self):
    self.headers = map(cleanHeader, self.headers)
    seen_headers = set()
    for h in self.headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)




def parseLines(lines, table_name, table_ct=0):
  tabs = []

  hrule_index = 0
  for i, line in enumerate(lines):
    if line[:2] == "--":
      hrule_index = i
                                                                                                                                                             31,0-1        Top
import os

class Table:
  def __init__(self):
    self.name = ''
    self.headers = []
    self.rows = []


class TableFactory:
  def isCleanChar(c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  def cleanHeader(h):
    h = ''.join([c for c in h if isCleanChar(c)])
    if h == "ORDER":
      h = h.capitalize() + 'X'
    return h

  def cleanHeaders(self):
    self.headers = map(cleanHeader, self.headers)
    seen_headers = set()
    for h in self.headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)

  def tablesFromLines(lines, table_name, table_ct=0):
    tabs = []

    hrule_index = 0
    for i, line in enumerate(lines):
      if line[:2] == "--":
        hrule_index = i
        break

    if hrule_index == 0:
    return []

  hrule_line = lines[hrule_index]
  col_widths = [len(rule) for rule in hrule_line.split()]
  num_cols = len(col_widths)

  head_line = lines[hrule_index - 1]
  heads = head_line.split()

  if len(heads) != num_cols:
    raise ValueError("different number of headers than columns")

  table_rows = []
  for i, line in enumerate(lines[hrule_index+1:]):
    if not line.strip():
      tabs += parseLines(lines[hrule_index+1+i:], table_name, table_ct+1)
      break

    offset = 0
    cells = []
    rline = line.rstrip()
    for col_width in col_widths:
      cell = rline[offset:offset+col_width].strip().decode('latin-1')
      cells.append(cell)
      offset += col_width
    table_rows.append(tuple(cells))

  tab_name = "{}{:02}".format(table_name, table_ct)
  tabs.append(Table(tab_name, heads, table_rows))
  return tabs


  def tablesFromFile(tab_file):
    with open(tab_file, 'r') as f:
      lines = f.readlines()
    tab_name = '_'.join(os.path.basename(tab_file).split('.')[:-1])
    return parseLines(lines, tab_name)




import os

class Table:
  def __init__(self, name='', headers=[], rows=[]):
    self.name = name
    self.headers = headers
    self.rows = rows


class TableFactory:
  def isCleanChar(self, c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  def cleanHeader(self, h):
    h = ''.join([c for c in h if isCleanChar(c)])
    if h == "ORDER":
      h = h.capitalize() + 'X'
    return h

  def cleanHeaders(self):
    self.headers = map(cleanHeader, self.headers)
    seen_headers = set()
    for h in self.headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)

  def tablesFromLines(lines, table_name, table_ct=0):
    tabs = []

    hrule_index = 0
    for i, line in enumerate(lines):
      if line[:2] == "--":
        hrule_index = i
        break

    if hrule_index == 0:
import os

class Table:
  def __init__(self, name='', headers=[], rows=[]):
    self.name = name
    self.headers = headers
    self.rows = rows


class TableFactory:
  def isCleanChar(self, c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  def cleanHeader(self, h):
    h = ''.join([c for c in h if isCleanChar(c)])
    if h == "ORDER":
      h = h.capitalize() + 'X'
    return h

  def cleanHeaders(self):
    self.headers = map(cleanHeader, self.headers)
    seen_headers = set()
    for h in self.headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)

  def tablesFromLines(lines, table_name, table_ct=0):
    tabs = []

    hrule_index = 0
    for i, line in enumerate(lines):
      if line[:2] == "--":
        hrule_index = i
        break

    if hrule_index == 0:

    hrule_line = lines[hrule_index]
    col_widths = [len(rule) for rule in hrule_line.split()]
    num_cols = len(col_widths)

    head_line = lines[hrule_index - 1]
    heads = head_line.split()

    if len(heads) != num_cols:
      raise ValueError("different number of headers than columns")

    table_rows = []
    for i, line in enumerate(lines[hrule_index+1:]):
      if not line.strip():
        tabs += parseLines(lines[hrule_index+1+i:], table_name, table_ct+1)
        break

      offset = 0
      cells = []
      rline = line.rstrip()
      for col_width in col_widths:
        cell = rline[offset:offset+col_width].strip().decode('latin-1')
        cells.append(cell)
        offset += col_width
      table_rows.append(tuple(cells))

    tab_name = "{}{:02}".format(table_name, table_ct)
    tabs.append(Table(name=tab_name, headers=heads, rows=table_rows))
    return tabs

  def tablesFromFile(tab_file):
    with open(tab_file, 'r') as f:
      lines = f.readlines()
    tab_name = '_'.join(os.path.basename(tab_file).split('.')[:-1])
    return parseLines(lines, tab_name)






                                                                                                                                                             73,3          Bot

    head_line = lines[hrule_index - 1]
    heads = head_line.split()

    if len(heads) != num_cols:
      raise ValueError("different number of headers than columns")

    table_rows = []
    for i, line in enumerate(lines[hrule_index+1:]):
      if not line.strip():
        tabs += parseLines(lines[hrule_index+1+i:], table_name, table_ct+1)
        break

      offset = 0
      cells = []
      rline = line.rstrip()
      for col_width in col_widths:
        cell = rline[offset:offset+col_width].strip().decode('latin-1')
        cells.append(cell)
        offset += col_width
      table_rows.append(tuple(cells))

    tab_name = "{}{:02}".format(table_name, table_ct)
    tabs.append(Table(name=tab_name, headers=heads, rows=table_rows))
    return tabs

  def tablesFromFile(tab_file):
    with open(tab_file, 'r') as f:
      lines = f.readlines()
    tab_name = '_'.join(os.path.basename(tab_file).split('.')[:-1])
    return parseLines(lines, tab_name)









  def cleanHeaders(self):



mport wget
import zipfile
import os
import glob
import sqlite3
import csv
import parse

DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(DIR, 'raw_data')
ZIP_DIR = os.path.join(DATA_DIR, 'zip_files')
CSV_DIR = os.path.join(DATA_DIR, 'csv_files')
META_DIR = os.path.join(DATA_DIR, 'meta')
DB = os.path.join(DIR, "bird_survey_db.sqlite3")

for d in [DATA_DIR, ZIP_DIR, CSV_DIR, META_DIR]:
  if not os.path.isdir(d):
    os.mkdir(d)

url = 'ftp://ftpext.usgs.gov/pub/er/md/laurel/BBS/DataFiles/50-StopData/1997ToPresent_SurveyWide'
files = [ 'Fifty1.zip', 'Fifty2.zip', 'Fifty3.zip', 'Fifty4.zip', 'Fifty5.zip',
    'Fifty6.zip', 'Fifty7.zip', 'Fifty8.zip', 'Fifty9.zip', 'Fifty10.zip' ]

for f in files:
  file_url = '/'.join([url, f])
  file_local = os.path.join(ZIP_DIR, f)

  print("Downloading: {}".format(file_url))
  wget.download(file_url, file_local)
  print("File downloaded.")

  print("Unzipping...")
  zip_ref = zipfile.ZipFile(file_local, 'r')
  zip_ref.extractall(CSV_DIR)
  zip_ref.close()
  print("Done.")

table_name = "fifty_stops"
id_name = "id"
col_names = [
    "RouteDataID", "countrynum", "statenum", "Route", "RPID", "year", "AOU"
t.py" 182L, 5593C                                                                                                                                            1,1           Top

table_name = "fifty_stops"
id_name = "id"
col_names = [
    "RouteDataID", "countrynum", "statenum", "Route", "RPID", "year", "AOU"
    ]
stop_names = [ "Stop{}".format(i) for i in range(1,51) ]
all_col_names = col_names + stop_names

print("Creating database...")
con = sqlite3.connect(DB)
cur = con.cursor()
print("Creating table...")
cur.execute("CREATE TABLE IF NOT EXISTS {tn} ({id_name} INTEGER PRIMARY KEY)".format(
  tn=table_name, id_name=id_name))

print("Adding columns...")
for col in col_names:
  cur.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
            .format(tn=table_name, cn=col, ct="TEXT"))
for col in stop_names:
  cur.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
            .format(tn=table_name, cn=col, ct="INTEGER"))

csv_files = glob.glob(os.path.join(CSV_DIR, '*.csv'))
for csv_file in csv_files:
  db_rows = []
  print("Parsing CSV file {}".format(csv_file))
  with open(csv_file, 'rb') as f:
    rows = csv.DictReader(f)
    for row in rows:
      db_rows.append(tuple([row[key] for key in all_col_names]))

  print("Inserting {} records into the database...".format(len(db_rows)))
  insert_command = "INSERT INTO {tn} ({cns}) VALUES ({qs})".format(
      tn=table_name,
      cns=', '.join(all_col_names),
      qs=', '.join(['?' for i in all_col_names]))
  cur.executemany(insert_command, db_rows)
  print("Records inserted.")

                                                                                                                                                             38,1          25%
  print("Records inserted.")

print("Committing changes...")
con.commit()
print("All records committed.")




url = 'ftp://ftpext.usgs.gov/pub/er/md/laurel/BBS/DataFiles'
files = [ 'SpeciesList.txt', 'BBSStrata.txt', 'BCR.txt', 'RunProtocolID.txt' ]

for f in files:
  file_url = '/'.join([url, f])
  file_local = os.path.join(META_DIR, f)

  print("Downloading: {}".format(file_url))
  wget.download(file_url, file_local)
  print("File downloaded.")



txt_files = glob.glob(os.path.join(META_DIR, '*.txt'))
for txt_file in txt_files:
  tab = parse.parse(txt_file)
  print("Creating table for {}...".format(os.path.basename(txt_file)))
  cur.execute("CREATE TABLE IF NOT EXISTS {tn} ({id_name} INTEGER PRIMARY KEY)".format(
    tn=tab.name, id_name=id_name))

  print("Adding columns...")
  for col in tab.headers:
    try:
      cur.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
                .format(tn=tab.name, cn=col, ct="TEXT"))
    except sqlite3.OperationalError as e:
      print("WARNING: {}".format(e))

  print("Inserting {} records into the database...".format(len(tab.rows)))
  insert_command = "INSERT INTO {tn} ({cns}) VALUES ({qs})".format(
      tn=tab.name,
      cns=', '.join(tab.headers),
