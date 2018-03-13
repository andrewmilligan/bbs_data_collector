import wget
import zipfile
import os
import glob
import sqlite3
import csv
import table


class BBS:

  def __init__(self, wd=None):

    if wd is None:
      self.DIR = os.getcwd()
    else:
      if not os.path.isdir(wd):
        raise ValueError("working directory '{}' does not exist".format(wd))
      self.DIR = os.path.abspath(wd)

    self.DATA_DIR = os.path.join(DIR, 'data')
    self.FIFTY_STOP_DIR = os.path.join(DATA_DIR, 'fifty_stops')
    self.META_DIR = os.path.join(DATA_DIR, 'meta')
    self.DIRECTORIES = [self.DATA_DIR, self.FIFTY_STOP_DIR, self.META_DIR]


    self.base_uri = 'ftp://ftpext.usgs.gov/pub/er/md/laurel/BBS/DataFiles'
    
    self.fifty_stop_uri = '50-StopData/1997ToPresent_SurveyWide'
    self.fifty_stop_files = [ 'Fifty1.zip', 'Fifty2.zip', 'Fifty3.zip',
        'Fifty4.zip', 'Fifty5.zip', 'Fifty6.zip', 'Fifty7.zip', 'Fifty8.zip',
        'Fifty9.zip', 'Fifty10.zip' ]

    self.DB = os.path.join(DIR, "bird_survey_db.sqlite3")

    self.fifty_stop_table = "fifty_stops"
    self.ID_VAR = "id"
    self.FIFTY_STOP_TEXT_COLS = [
        "RouteDataID", "countrynum", "statenum", "Route", "RPID", "year", "AOU"
        ]
    self.FIFTY_STOP_STOP_COLS = [ "Stop{}".format(i) for i in range(1,51) ]
    self.FIFTY_STOP_COLS = self.FIFTY_STOP_TEXT_COLS + self.FIFTY_STOP_STOP_COLS



  def initDirectories(self):
    for d in self.DIRECTORIES:
      if not os.path.isdir(d):
        os.mkdir(d)

  def makeAbsolutePath(self, *rel_path_pieces):
    return '/'.join([self.base_uri] + [r for r in rel_path_pieces if r])

  def unzipFile(self, file_local, unzip_dir):
    if not os.path.isdir(unzip_dir):
      raise IOError("unzip target dir '{}' does not exist".format(unzip_dir))
    print("Unzipping {}...".format(file_local))
    zip_ref = zipfile.ZipFile(file_local, 'r')
    zip_ref.extractall(unzip_dir)
    zip_ref.close()
    print("Done.")

  def fetchFile(self, file_uri, file_local, unzip_dir=None):
    print("Downloading: {}".format(file_url))
    wget.download(file_uri, file_local)
    print("File downloaded to {}.".format(file_local))
    if unzip_dir is not None:
      self.unzipFile(file_local, unzip_dir)

  def fetchFileList(self, uri, files, local_dir, unzip_dir=None):
    for f in files:
      file_uri = self.makeAbsoluteURI(uri, f)
      file_local = os.path.join(local_dir, f)
      self.fetchFile(file_uri, file_local, unzip_dir=unzip_dir)

  def fetchFiftyStopFiles(self):
    self.fetchFileList(self.fifty_stop_uri, self.fifty_stop_files,
        self.FIFTY_STOP_DIR, self.FIFTY_STOP_DIR)

  def fetchMetaTxtFiles(self):
    self.fetchFileList(self.meta_txt_uri, self.meta_txt_files,
        self.META_DIR)

  def fetchMetaCsvFiles(self):
    self.fetchFileList(self.meta_csv_uri, self.meta_csv_files,
        self.META_DIR, self.META_DIR)
      

