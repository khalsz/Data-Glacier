

# from google.colab import drive
# drive.mount('/content/drive')

import os
import pandas as pd
os.chdir('C:/Users/khalsz/Documents/Leicester Uni Actvt/glacier internship')

# import zipfile
# zipfile.is_zipfile('/content/drive/MyDrive/data_glacier/data/archive (10).zip')

datadir = 'C:/Users/khalsz/Documents/Leicester Uni Actvt/glacier internship/data'

from os.path import join, isfile
import zipfile
datadir = '/content/drive/MyDrive/data_glacier/data'

def unzip_file(path): 
  files = os.listdir(path)
  for f in files: 
    if  zipfile.is_zipfile(join(path, f)) and f.endswith('zip'): 
      filepath = join(path, f)
      zip_file = zipfile.ZipFile(filepath)
      for names in zip_file.namelist(): 
        zip_file.extract(names, path)
      zip_file.close()
unzip_file(datadir)

import logging 
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth

gauth = GoogleAuth()
  
# Creating local webserver and auto

gauth.LocalWebserverAuth()       
drive = GoogleDrive(gauth)


%%writefile pipeline.py
import logging 
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re


def read_val_file(filepath):
  with open(filepath, 'r') as fl: 
    try: 
      return yaml.safe_load(fl)
    except yaml.YAMLError as failed: 
      logging.error(failed)

def replacer(string, char):
    pattern = re.compile('[\W] {2,}')
    string = re.sub(pattern, char, string) 
    return string

def col_name_val (df, table_config): 
  df.columns = df.columns.str.lower()
  df.columns = df.columns.str.replace('[^\w]', '_', regex = True)
  df.columns = list(map(lambda x: x.strip('_'), list(df.columns)))
  df.columns = sorted(list(map( lambda x: x.lower(), df.columns)))
  expected_col = sorted(list(map(lambda x : x.lower(), table_config['columns'])))
  if len(df.columns) == len(expected_col) and list(df.columns) == list(expected_col): 
    print('Validation Passed for Column length and name')
    print('Data Info is: {}'.format(df.info()))
    return 1
  else: 
    print('Validation failed')
    mismatched_columns = list(set(df.columns).difference(expected_col))
    print('The following file Columns are not in YAML validation file', mismatched_columns)
    missing_YAML_cols = list(set(expected_col).difference(df.columns))
    print('The following YAML validation columns are missing in uploaded file', missing_YAML_cols)
    logging.info(f'df columns: {df.columns}')
    logging.info(f'expected columns: {expected_col}')
    return 0


addname = re.compile('[\d]+')

# Commented out IPython magic to ensure Python compatibility.
%%writefile file.yaml
file_type: csv
dataset_name: r'\^test_data\d*'
file_name: r'\^test_data\d*'
table_name: edsurv
inbound_delimiter: ","
outbound_delimiter: "|"
skip_leading_rows: 1
columns: 
    - index
    - date
    - actual_mean_temp
    - actual_min_temp
    - actual_max_temp

import pipeline as pp
config_data = pp.read_val_file("file.yaml")



os.chdir(datadir)

import pandas as pd
df_sample = pd.read_csv("test_data1.csv",delimiter=',')
df_sample.head()

# read the file using config file
file_type = config_data['file_type']
datafiles = [f for f in os.listdir(datadir) if f.endswith('.csv')]
df= {}
#print("",source_file)
for f, r in enumerate(datafiles):
  df["group" + str(f)] = pd.read_csv(r,config_data['inbound_delimiter'])

del df['group55']

downloaddir = '[https://drive.google.com/drive/folders/'

for keys in df.keys(): 
    if (pp.col_name_val(df[keys],config_data) == 0): 
        print('Validation failed')
    else: 
        fil = drive.CreateFile({'title': 'x'})
        fil.SetContentFile(os.path.join(downloaddir, df[keys]))
        fil.Upload()
  

    fil = None