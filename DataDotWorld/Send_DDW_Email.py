import json
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import sys
from sqlalchemy.engine import URL
import requests
from json import loads
from send_email import send_email

dev = "NO"

if dev == "YES":
    from dotenv import load_dotenv
    load_dotenv(".env")

# Load Datawarehouse Credentials
wh_host = os.getenv("WH_HOST")
wh_db = os.getenv("WH_DB")
wh_un = os.getenv("WH_USER")
wh_pw = os.getenv("WH_PASS")

# Import table variables
dept = os.getenv('DEPT')
table = os.getenv('TABLE')
source = os.getenv('SOURCE')
schema = os.getenv('SCHEMA', 'Master')

# DDW variables
auth_token = os.getenv('DW_AUTH_TOKEN')

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
catalog_IRI = catalog_records[['id', 'encodedIri', 'collections']]
catalog_IRI = pd.concat([catalog_IRI.drop(['collections'], axis=1),
                         catalog_IRI['collections'].apply(pd.Series)], axis=1)

df_tables_n = df_tables.merge(catalog_IRI, left_on=['Datatable_Title_value', 'CollectionName_value'],
                              right_on=['id', 'collectionId'])

stewards_table = df_tables_n[['DataSteward_value','DataSteward_EMAIL_value']].copy()
stewards_table['DataSteward_EMAIL_value'] = stewards_table['DataSteward_EMAIL_value'].apply(str.lower)
stewards_table = stewards_table.drop_duplicates()
# USED FOR TESTING, COMMENT/DELETE
# stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus',
#                                                                           'Ali Greenholt', 'Geoffrey Arnold'])]
stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus'])]
# Opening the html file
HTMLFile = open("EmailTemplate.html", "r")
EmailTemplate = HTMLFile.read()


def message_creater(stewardess, tables, template):
    link_rows = tables[tables['DataSteward_value'] == stewardess]
    link_list = ["""<br><li> <a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>""".
                 format(link_rows['encodedIri'][row],
                        link_rows['Datatable_Title_value'][row]) for row in link_rows.index]
    link_list = "".join(link_list)
    message = template.format(link_list)
    return message


for steward in stewards_table['DataSteward_value']:
    Email_Message = message_creater(steward, df_tables_n, EmailTemplate)
    Steward_Email = stewards_table.loc[stewards_table['DataSteward_value'] == steward, 'DataSteward_EMAIL_value'].values[0]
    send_email(subject='Test_DDW_Email', to_emails=Steward_Email,
               message=Email_Message)
