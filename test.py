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

