#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import sqlalchemy as sa
import pyodbc
import json
import sys
import argparse
import os

# Load Passed Variables
username = os.environ['USER']
password = os.environ['PASS']
host = os.environ['HOST']
database = os.environ['DATABASE']
schema = os.environ['SCHEMA']
dept = os.environ['DEPT']
table = os.environ['TABLE']

# Warehouse Constants
wh_host = "DEVSQL17.County.Allegheny.Local\CountyStat_DW"
wh_database = 'DataWarehouse'

# Load Data Warehouse Credentials
sa_user=os.environ["SA_USER"]
sa_password=os.environ["SA_PASS"]

wh_conn_string = "mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(sa_user, sa_password, wh_host, wh_database)
conn = sa.create_engine(wh_conn_string)

sql = """SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM {}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}_{}_{}'""".format(wh_database, dept, database, table)

df = pd.read_sql(sql, conn)

schema = json.loads(os.environ['SCHEMA'])

random_test = 'SELECT TOP 10 PERCENT * FROM {}.Preload.{}_{}_{} order by newid()'.format(wh_database, dept, database, table)
random = pd.read_sql(random_test, conn)

for col in col_dict:
    test = df[df[r'COLUMN_NAME'] == col]
    test = test.reset_index(drop=True)

    if test.shape[0] == 0:
        sys.exit('Expected column {} from dataset schema is missing.'.format(col))
    else:
        print('{} column is present.'.format(col))
        
    data_type_expected = str(col_dict[col]['type'])
    data_type_actual = str(test[r'DATA_TYPE'][0])
    
    # Check for NA's
    if 'na' in col_dict[col].keys():
        if col_dict[col]['na'] == False:
            if random[col].isnull().any():
                sys.exit('{} column has NaN values, expected none.'.format(col))
            else:
                print('{} column has no NaN values, as expected.')
    if data_type_expected in ('date', 'datetime'):
        dt_format = col_dict[col][r'format']
        try:
            random[col] =  pd.to_datetime(random[col], format = dt_format)
            print('{} column has the correct date format ({}).'.format(col, dt_format))
        except: # catch *all* exceptions
            e = repr(sys.exc_info())
            sys.exit('Formatting Error: {}'.format(e))
    elif data_type_actual != data_type_expected:
        sys.exit('Column {} is {}, expected {}.'.format(col, data_type_actual, data_type_expected))
    else:
        print('{} column is the correct type ({}).'.format(col, data_type_expected))
