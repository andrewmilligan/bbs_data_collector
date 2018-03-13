import os

## Table
#
#  This is a table class to store a SQL table defined in a BBS meta .txt file.
#  A Table has a table name, a list of column headers, and a list of tuples
#  representing the table rows.
#
class Table:
  def __init__(self, name='', headers=[], rows=[]):
    self.name = name
    self.headers = headers
    self.rows = rows


## TableFactory
#
#  A factory class to parse BBS meta .txt files and produce Table objects. The
#  main parsing functions return lists of tables, because BBS meta .txt files
#  may contain multiple tables.
#
class TableFactory:
  ## isCleanChar
  #
  #  Helper function to 
  def isCleanChar(c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  def cleanHeader(h):
    h = ''.join([c for c in h if self.isCleanChar(c)])
    if h == "ORDER":
      h = h.capitalize() + 'X'
    return h

  def cleanHeaders(self, headers):
    clean_headers = map(self.cleanHeader, headers)
    seen_headers = set()
    for h in clean_headers:
      if not h:
        raise ValueError("header with no clean characters")
      if h in seen_headers:
        raise ValueError("duplicate header: '{}'".format(h))
      seen_headers.add(h)
    return clean_headers

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
    heads = self.cleanHeaders(head_line.split())

    if len(heads) != num_cols:
      raise ValueError("different number of headers than columns")

    table_rows = []
    for i, line in enumerate(lines[hrule_index+1:]):
      if not line.strip():
        tabs += self.tablesFromLines(lines[hrule_index+1+i:],
            table_name, table_ct+1)
        break

      offset = 0
      cells = []
      rline = line.rstrip()
      for col_width in col_widths:
        cell = rline[offset:offset+col_width].strip().decode('latin-1')
        cells.append(cell)
        offset += col_width
      table_rows.append(tuple(cells))

    if table_ct > 0:
      tab_name = "{}{:02}".format(table_name, table_ct)
    else:
      tab_name = table_name
    tabs.append(Table(tab_name, heads, table_rows))
    return tabs


  def tablesFromFile(tab_file):
    with open(tab_file, 'r') as f:
      lines = f.readlines()
    tab_name = '_'.join(os.path.basename(tab_file).split('.')[:-1])
    return self.tablesFromLines(lines, tab_name)
