#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import sqlalchemy as sa
from sodapy import Socrata
from sqlalchemy import event
from sqlalchemy.engine import URL
from sqlalchemy import types

# Env Variables
# Data Vars
dept = os.getenv('dept')
source = os.getenv('source')
table = os.getenv('table')
schema_t = os.getenv('schema', default='Staging')
table_name = f"{dept}_{source}_{table}"

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")
dev = os.getenv("DEV", "no")

# Build Connection & Query Warehouse
if dev == "YES":
    wh_conn_string = URL.create(
        "mssql+pyodbc",
        username=wh_un,
        password=wh_pw,
        host=wh_host,
        database=wh_db,
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "trusted_connection": "yes"
        },
    )
else:
    wh_conn_string = URL.create(
        "mssql+pyodbc",
        username=wh_un,
        password=wh_pw,
        host=wh_host,
        database=wh_db,
        query={
            "driver": "ODBC Driver 17 for SQL Server"
        },
    )
engine = sa.create_engine(wh_conn_string)

# Socrata Vars
# Credentials
socrata_token = os.getenv("sodapy_apptoken")
# Identifier
identifier = os.getenv("socrata_data_identifier")
# Domain
domain = os.getenv('domain')
# Filter
filter_con = os.getenv("filter")


# Row Count
def get_row_count(dom, conn):
    client = Socrata(dom, socrata_token)
    count_result = client.get(conn,
                              where=filter_con,
                              select="count('Business Name')")
    meta_amount = pd.DataFrame.from_dict(count_result)
    row_count_result = int(meta_amount.iloc[0]['count_Business_Name'])
    return row_count_result


def pull_data(dom, conn, rowcount, condition=''):
    client = Socrata(dom, socrata_token)
    client.timeout = 60
    lst = []
    loop_size = 3000
    num_loops = rowcount // loop_size + 1
    i: int
    for i in range(num_loops):
        results = client.get(
            conn,
            where=condition,
            limit=loop_size,
            offset=loop_size * i
        )
        print("\n> Loop number: {}".format(i))

        for result in results:
            lst.append(result)

    # df_return = pd.DataFrame.from_dict(lst)
    df_return = pd.DataFrame(lst)
    return df_return


def load(dfo, eng):
    sql = sa.text(
        "SELECT COLUMN_NAME, DATA_TYPE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_NAME = 'Epidemiology_DOH_2025_Births' AND TABLE_SCHEMA = 'ACHD'"
    )
    with eng.begin() as conn:
        result = conn.execute(sql).fetchall()
    conn.close()
    my_dict = dict(result)
    new_mapping = {
        'longitude': sa.types.Float(asdecimal=True),
        'latitude': sa.types.Float(asdecimal=True)
    }
    dfo.to_sql(name=table_name, con=eng, index=False, if_exists="replace", schema=schema_t, dtype=new_mapping, chunksize=5000)


def parse_coordinates(coord_col):
    longs = []
    lats = []
    for i in range(0, coord_col.shape[0]):
        if pd.isna(coord_col[i]):
            long = pd.NA
            lat = pd.NA
        else:
            coords = coord_col[i]['coordinates']
            long = coords[0]
            lat = coords[1]
        longs.append(long)
        lats.append(lat)
    return longs, lats


def convert_types(dfo):
    dfo = dfo.convert_dtypes()
    num_cols = []
    date_cols = []
    drop_cols = []
    for column in dfo.columns:
        if ':@computed' in column:
            drop_cols.append(column)
        elif dfo[column].str.isnumeric().sum() == len(dfo[column]) - dfo[column].isna().sum():
            num_cols.append(column)
        elif 'date' in column:
            date_cols.append(column)
        elif 'latitude' in column and 'longitude' in column:
            longs, lats = parse_coordinates(dfo[column])
            dfo['longitude'] = longs
            dfo['latitude'] = lats
            dfo.drop(columns=column, inplace=True)

    dfo[num_cols] = dfo[num_cols].apply(pd.to_numeric, errors='coerce')
    dfo[date_cols] = dfo[date_cols].apply(pd.to_datetime, errors='coerce')
    dfo.drop(columns=drop_cols, inplace=True)
    print(dfo.dtypes)
    return dfo


row_count = get_row_count(domain, identifier)
df = pull_data(domain, identifier, row_count, filter_con)
df = convert_types(df)
load(df, eng=engine)
