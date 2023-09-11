import json
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import sys
from sqlalchemy.engine import URL
import requests
from json import loads

dev = "YES"

if dev == "YES":
    from dotenv import load_dotenv

    load_dotenv(".env")

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")

# Import table variables
dept = os.getenv('dept')
table = os.getenv('TABLE')
source = os.getenv('SOURCE')
schema = os.getenv('schema', 'Master')

# DDW variables
auth_token = os.getenv('dw_auth_token')

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

table_name = "{}_{}_{}".format(dept, source, table)
df = pd.read_sql_table(table_name, engine, schema=schema)

df_tables = df.groupby("Datatable_Title_value").sample(1).drop(['ColumnTitle_value', 'CatalogObject_value',
                                                                'resourceType_value'], axis=1)
df_collections = df_tables["CollectionName_value"].unique().tolist()

catalog_records = pd.DataFrame()

# API call
headers = {
    'Accept': "application/json",
    'Authorization': "Bearer {}".format(auth_token)
}

for collection in df_collections:
    response = requests.get(
        "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=1000".format(collection),
        headers=headers)
    d = loads(response.text)
    df_d = pd.DataFrame(d['records'])
    catalog_records = pd.concat([df_d, catalog_records], ignore_index=True)

catalog_records = catalog_records.explode('collections')
df_tables_n = df_tables.merge(catalog_records, left_on='Datatable_Title_value', right_on='id')

import ast


def only_dict(d):
    '''
    Convert json string representation of dictionary to a python dict
    '''
    return ast.literal_eval(d)


def list_of_dicts(ld):
    '''
    Create a mapping of the tuples formed after
    converting json strings of list to a python list
    '''
    return dict([(list(d.values())[1], list(d.values())[0]) for d in ast.literal_eval(ld)])


# Need to make this work row-wise
A = pd.json_normalize(catalog_records['collections'].apply(only_dict).tolist()).add_prefix('columnA.')
B = pd.json_normalize(catalog_records['collections'].apply(list_of_dicts).tolist()).add_prefix('collections.pos.')
New_Catalog = catalog_records.join([A, B])
# dupes = df_tables_n['Datatable_Title_value'].duplicated()
# dupe_rows = df_tables_n[dupes]
# dupe_row_t = df_tables_n[df_tables_n.Datatable_Title_value == 'HumanResources_JDE_CurrentEmployeeDetails_V']
# dview = df[df.Datatable_Title_value == 'HumanResources_JDE_CurrentEmployeeDetails_V']
