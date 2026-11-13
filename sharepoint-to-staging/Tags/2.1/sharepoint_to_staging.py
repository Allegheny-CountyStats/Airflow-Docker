#!/usr/bin/env python
# coding: utf-8
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import pandas as pd
import os
import random
import time
import uuid
import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError
import re

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")
schema = os.getenv("schema", 'Staging')

sheet = os.getenv("sheet", '')
skip = int(os.getenv('skip', 0))
filetype = os.getenv("filetype", '')
snake_case = os.getenv("snakecase", "NO")

# Build Connection & Query Warehouse
connection_url = URL.create(
    "mssql+pyodbc",
    username=wh_un,
    password=wh_pw,
    host=wh_host,
    database=wh_db,
    query={
        "driver": "ODBC Driver 17 for SQL Server"
    },
)
# fast_executemany batches inserts;
# hide_parameters keeps row values out of error messages and Airflow logs
engine = sa.create_engine(connection_url, fast_executemany=True, hide_parameters=True)


def quote_name(name):
    return '[' + name.replace(']', ']]') + ']'


def retry_deadlock(fn, what, max_attempts):
    """Run fn(); if SQL Server picks it as a deadlock victim (SQLSTATE 40001) the transaction was rolled
    back, so retry after a random wait."""
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except DBAPIError as e:
            sqlstate = e.orig.args[0] if e.orig is not None and e.orig.args else None
            if sqlstate != '40001' or attempt == max_attempts:
                raise
            wait = random.uniform(5, 20) * attempt
            print(f'Deadlock victim during {what} (attempt {attempt}/{max_attempts}); retrying in {wait:.0f}s')
            time.sleep(wait)


def swap_statements(schema, load_name, table_name):
    """SQL Server: replace schema.table_name with schema.load_name in one transaction."""
    return [
        (f"DROP TABLE IF EXISTS {quote_name(schema)}.{quote_name(table_name)}", {}),
        ("EXEC sp_rename :old, :new", {'old': f"{schema}.{load_name}", 'new': table_name}),
    ]


def write_staging(df, engine, schema, table_name, max_attempts=5):
    """Load df into schema.table_name without holding DDL locks during the insert.

    pandas' to_sql(if_exists='replace') runs drop + create + every insert + a schema-wide catalog scan in
    one transaction, so parallel loads into the same schema deadlock (error 1205). Instead:
    1. create an empty private table (short, committed),
    2. insert into it (long, touches only the private table),
    3. drop the old table and rename the private one into place (short, atomic).
    """
    # Lower-case private name: pandas skips its schema-wide case-sensitivity catalog scan for lower-case names
    load_name = f"{table_name}__load_{uuid.uuid4().hex[:8]}".lower()
    swapped = False
    try:
        retry_deadlock(lambda: df.head(0).to_sql(name=load_name, schema=schema, con=engine,
                                                 if_exists='fail', index=False),
                       f'create {schema}.{load_name}', max_attempts)
        df.to_sql(name=load_name, schema=schema, con=engine, if_exists='append', index=False, chunksize=10000)

        def swap():
            with engine.begin() as conn:
                for stmt, params in swap_statements(schema, load_name, table_name):
                    conn.execute(sa.text(stmt), params)
        retry_deadlock(swap, f'swap into {schema}.{table_name}', max_attempts)
        swapped = True
    finally:
        if not swapped:
            with engine.begin() as conn:
                conn.execute(sa.text(f"DROP TABLE IF EXISTS {quote_name(schema)}.{quote_name(load_name)}"))

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

# Save location
dept = os.getenv('dept')
source = os.getenv('source', 'Sharepoint')
table = os.getenv('table')
table_name = '{}_{}_{}'.format(dept, source, table)

# Information
drive = os.getenv('drive_id')
file_id = os.getenv('file_id')
drive_type = os.getenv('drive_type', 'drives')

auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)

# Auth
token = oauth.fetch_token(
    token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
    scope='https://graph.microsoft.com/.default',
    auth=auth)

bearer = "Bearer {}".format(token['access_token'])
headers = {'authorization': bearer}

if drive_type == 'drives':
    url = "https://graph.microsoft.com/v1.0/drives/{}/items/{}/content".format(drive, file_id)
else:
    url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/content".format(drive, file_id)

file = requests.request("GET", url, data="", headers=headers)
file.raise_for_status()

if filetype == 'x':
    filename = 'test.xlsx'
elif filetype == 'csv':
    filename = 'test.csv'
else:
    filename = 'test.xls'

with open(filename, 'wb') as output:
    output.write(file.content)

if filetype == 'csv':
    df = pd.read_csv(filename)
else:
    if sheet == '':
        df = pd.read_excel(filename)
    elif sheet == 'all':
        xls = pd.ExcelFile(filename)
        df = pd.DataFrame()
        for i in xls.sheet_names:
            temp = pd.read_excel(filename, sheet_name=i)
            df = df.append(temp)
    else:
        if skip == 0:
            df = pd.read_excel(filename, sheet_name=sheet)
        else:
            df = pd.read_excel(filename, sheet_name=sheet, skiprows=range(0, skip))
    if filetype != 'x':
        df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

print(df.head())

if snake_case == "YES":
    def to_snake_case(name):
        name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        name = re.sub(r"[!@#]", "", name)
        name = re.sub(r" ", "_", name)
        name = re.sub(r"^_{,2}", "", name)
        name = re.sub(r"_{,2}$", "", name)
        return name.lower()


    df.columns = [to_snake_case(col) for col in df.columns]

write_staging(df, engine, schema, table_name, int(os.getenv('deadlock_retries', 5)))
