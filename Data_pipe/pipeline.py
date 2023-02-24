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
    print('Data Info is: {}'.format(datainfo = df.info()))
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
