#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import sqlalchemy as sa
import pymssql
import json
import sys
import argparse

parser = argparse.ArgumentParser(description='App to move check the schema of a dataset')

parser.add_argument('-path', 
                    action="store", 
                    dest="path",
                    type=str,
                    default = 'schema.json'
                    help="File path of schema.")
parser.add_argument('-db', 
                    action="store", 
                    dest="db",
                    type=str,
                    help="Datawarehouse db name.")
parser.add_argument('-schema', 
                    action="store", 
                    dest="schema",
                    type=str,
                    help="Datawarehouse schema name.")
parser.add_argument('-table', 
                    action="store", 
                    dest="table",
                    type=str,
                    help="Datawarehouse table name.")

# Load Data Warehouse Credentials
host=os.getenv("host")
user=os.getenv("user")
password=os.getenv("password")
database=os.getenv("database")

with open(args.path, encoding='utf-8-sig') as json_file:
    data = json.load(json_file)

col_dict = data[r'col_names']

conn = pymssql.connect(host=host, user=user, password=password, database=database)

sql = """SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
FROM {}.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}'""".format(args.db, args.table)

df = pd.read_sql(sql, conn)

random_test = 'SELECT TOP 10 PERCENT * FROM {}.{}.{} order by newid()'.format(args.db, args.schema, args.table)
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
                print('{} column has no NaN values, as expected.'.format(col))
    else:
        print("{} column has not been tested for NaN values".format(col))
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