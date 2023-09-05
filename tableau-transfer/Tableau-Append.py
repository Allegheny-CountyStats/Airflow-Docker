#!/usr/bin/env python
# coding: utf-8
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import tableauserverclient as TSC
import sys
from tableauhyperapi import TableName
import re
import gc
from sqlalchemy.engine import URL

dev = "YES"

if dev == "YES":
    from dotenv import load_dotenv

    load_dotenv("./tableau-transfer/.env")

# Env Variables
# Data Vars
dept = os.getenv('dept')
table = os.getenv('table')
schema = os.getenv('schema', 'Reporting')
column_q = os.getenv('column_q', '*')
fix_dates = os.getenv('fix_dates', 'yes')
int_requests = os.getenv('INT_REQ', '')
int_chunks = int(os.getenv('INT_CHUNKSIZE', 5000))

# Tableau Vars
if dev != "YES":
    name = os.getenv('name')
else:
    name = "Testing_{}".format(os.getenv('name'))

mode = os.getenv('mode', 'Append')
project_id = os.getenv('d', '')
if dev != "YES":
    project_name = os.getenv('project_name', dept)
else:
    project_name = "Site Archive"

site = os.getenv('site', 'CountyStats')
server = os.getenv('server', 'tableau')

# Load Tableau Credentials
tableau_username = os.getenv("ts_username")
tableau_password = os.getenv("ts_password")
tableau_TOKEN_name = os.getenv("TS_TOKEN_NAME")
tableau_TOKEN = os.getenv("TS_TOKEN")

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")

# Build Connection & Query Warehouse
if dev == "NO":
    connection_url = URL.create(
        "mssql+pyodbc",
        username=wh_un,
        password=wh_pw,
        host=wh_host,
        database=wh_db,
        query={
            "driver": "ODBC Driver 17 for SQL Server",
        },
    )

    engine = sa.create_engine(connection_url)
else:
    engine = sa.create_engine(
        "mssql+pyodbc://{}/{}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server".format(wh_host, wh_db))

# Pull Date and DateTime Columns
date_cols = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}' AND DATA_TYPE IN ('date', 'datetime', 'smalldatetime')""".format(table)

cols = pd.read_sql_query(date_cols, engine)

datecols = cols["COLUMN_NAME"].values.tolist()

int_cols = """SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{}' AND DATA_TYPE IN ('int')""".format(table)

cols = pd.read_sql_query(int_cols, engine)

intcols = cols["COLUMN_NAME"].values.tolist()

# Read and write table to hyper file

# Connect to Tableau Server
server = TSC.Server('https://{}.alleghenycounty.us'.format(server))
server.version = '3.3'
if dev == "NO":
    tableau_auth = TSC.TableauAuth(tableau_username, tableau_password, site_id=site)

    with server.auth.sign_in(tableau_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, name))
        req_option.filter.add(
            TSC.Filter(TSC.RequestOptions.Field.ProjectName, TSC.RequestOptions.Operator.Equals, project_name))
        all_datasources = server.datasources.get(req_option)
else:
    tableau_auth = TSC.PersonalAccessTokenAuth(tableau_TOKEN_name,
                                               tableau_TOKEN, site_id=site)

    with server.auth.sign_in(tableau_auth):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, name))
        req_option.filter.add(
            TSC.Filter(TSC.RequestOptions.Field.ProjectName, TSC.RequestOptions.Operator.Equals, project_name))
        all_datasources = server.datasources.get(req_option)
