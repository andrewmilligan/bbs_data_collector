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
  #  Helper function to make sure that characters in header names are safe.
  #
  def isCleanChar(self, c):
    return (c >= 'a' and c <= 'z') or \
        (c >= 'A' and c <= 'Z') or \
        (c >= '0' and c <= '9')

  ## cleanHeader
  #
  #  Helper function to strip out characters from a header name that aren't
  #  safe using the isCleanChar member function.
  #
  def cleanHeader(self, h):
    h = ''.join([c for c in h if self.isCleanChar(c)])
    return h

  ## cleanHeaders
  #
  #  Helper function to clean a whole list of headers and perform some simple
  #  checks that raise errors in the case of empty headers or duplicate
  #  headers.
  #
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

  ## tablesFromLines
  #
  #  Parses a list of lines from a BBS meta .txt file and produces a list of
  #  table objects (because a single file may contain multiple tables). A BBS
  #  SQL table should have the following properties:
  #    * it has exactly one line of headers
  #    * no header contains any whitespace
  #    * the header line is immediately followed by a horizontal rule made only
  #      of dashes ('-') and spaces
  #    * the horizontal rule is divided into as many contiguous groups of
  #      dashes as there are columns
  #    * each contiguous group of dashes starts at the beginning of a column
  #      and has length equal to or exceeding the widest cell in the column
  #    * a table may contain no lines consisting only of whitespace
  #  
  def tablesFromLines(self, lines, table_name, table_ct=0):
    tabs = []

    # Find the horizontal rule to figure out the column spacing. If there is
    # no rule, there's no table to parse
    hrule_index = 0
    for i, line in enumerate(lines):
      if line[:2] == "--":
        hrule_index = i
        break
    if hrule_index == 0:
      return []

    # Calculate column widths based on the horizontal rule
    hrule_line = lines[hrule_index]
    col_widths = [len(rule) for rule in hrule_line.split()]
    num_cols = len(col_widths)

    # Parse and clean the headers
    head_line = lines[hrule_index - 1]
    heads = self.cleanHeaders(head_line.split())

    # Ensure that there are an equal number of headers and columns
    if len(heads) != num_cols:
      raise ValueError("different number of headers than columns")

    # Parse out the rows based on the column widths
    table_rows = []
    for i, line in enumerate(lines[hrule_index+1:]):

      # If we have reached a line consisting only of whitespace, we have
      # reached the end of the current table. Try to parse more from the
      # remainder of the file.
      if not line.strip():
        tabs += self.tablesFromLines(lines[hrule_index+1+i:],
            table_name, table_ct+1)
        break

      # Pick out each cell by substringing with the column width
      offset = 0
      cells = []
      rline = line.rstrip()
      for col_width in col_widths:
        cell = rline[offset:offset+col_width].strip().decode('latin-1')
        cells.append(cell)
        offset += col_width
      table_rows.append(tuple(cells))

    # Append a new table object to the list and return
    if table_ct > 0:
      tab_name = "{}{:02}".format(table_name, table_ct)
    else:
      tab_name = table_name
    tabs.append(Table(tab_name, heads, table_rows))
    return tabs


  ## tablesFromFile
  #
  #  A convenience wrapper to parse a BBS meta .txt file directly. It uses the
  #  file name as the table name.
  #
  def tablesFromFile(self, tab_file):
    with open(tab_file, 'r') as f:
      lines = f.readlines()
    tab_name = '_'.join(os.path.basename(tab_file).split('.')[:-1])
    return self.tablesFromLines(lines, tab_name)
