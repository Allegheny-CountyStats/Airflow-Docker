import json
import os
import pandas as pd
import sqlalchemy as sa
import pantab
import sys
import urllib3.exceptions
from urllib3.exceptions import NewConnectionError
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session
import requests
from json import loads
from send_email import send_email
import time
from requests.exceptions import SSLError
from requests.adapters import HTTPAdapter, Retry
from sqlalchemy import delete, Table, MetaData, insert, select

# Change to "NO" when in Dev/Prod servers
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

# Message variables
email_filename = os.getenv('EMAIL_TEMPLATE')
email_subject = os.getenv('EMAIL_SUBJECT')
image_subfolder = os.getenv('IMAGE_SUBFOLDER')

# DDW variables
auth_token = os.getenv('DW_AUTH_TOKEN')

datadotworld = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
datadotworld.mount('http://', HTTPAdapter(max_retries=retries))

# Build Connection & Query Warehouse
if dev == "NO":
    print("Using CountyStat Username")
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

# Set table name for data import from Warehouse
table_name = "{}_{}_{}".format(dept, source, table)
df = pd.read_sql_table(table_name, engine, schema=schema)

# Gather unique table names
df_tables = df.groupby("Datatable_Title_value").sample(1).drop(['ColumnTitle_value', 'CatalogObject_value',
                                                                'resourceType_value'], axis=1)
# Gather unique collection names
df_collections = df_tables["CollectionName_value"].unique().tolist()

# API call
headers = {
    'Accept': "application/json",
    'Authorization': "Bearer {}".format(auth_token)
}

# Gather data sources to use for tables API call
while True:
    try:
        response = \
            datadotworld.get("https://api.data.world/v0/metadata/data/sources/alleghenycounty?size=1000", headers=headers)
        break
    except (SSLError, ConnectionError, TimeoutError):
        continue
d = loads(response.text)
df_datasources = pd.DataFrame(d['records'])

source_records = pd.DataFrame()
for i, row in df_datasources.iterrows():
    sourceid = row['id']
    attempt = 0
    while True:
        try:
            attempt += 1
            response = datadotworld.get(
                "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=20".
                format(sourceid),
                headers=headers)
            print(response.url, "[", attempt, "]")
            d = loads(response.text)
            df_d = pd.json_normalize(d, 'records')
            df_d['Source'] = sourceid
            break
        except (SSLError, ConnectionError, KeyError, NewConnectionError):
            continue
        except TimeoutError:
            time.sleep(1)
            continue

    source_records = pd.concat([df_d, source_records], ignore_index=True)
    while "nextPageToken" in response.json():
        next_page = d['nextPageToken']
        while True:
            try:
                response = datadotworld.get("""https://api.data.world/v0/{}""".format(next_page), headers=headers)
                print("Next page add: ", next_page)
                d = loads(response.text)
                df_d = pd.json_normalize(d, 'records')
                df_d['Source'] = sourceid
                break
            except (SSLError, ConnectionError, KeyError, NewConnectionError):
                continue
            except TimeoutError:
                time.sleep(1)
                continue
        source_records = pd.concat([df_d, source_records], ignore_index=True)
    time.sleep(0.1)

source_records_n = source_records[['title', 'collections', 'Source']]
source_records_n = source_records_n.explode('collections')
source_records_n['Collection'] = [d.get('collectionId') for d in source_records_n.collections]

df_tables = df_tables.merge(source_records_n[['title', 'Collection', 'Source']], how='left',
                            left_on=['Datatable_Title_value', 'CollectionName_value'],
                            right_on=['title', 'Collection'])

# Gather catalog records of database tables using collection name, which then gives table IRI for eventual url link
catalog_records = pd.DataFrame()

for collection in df_datasources['id']:
    attempt = 0
    while True:
        try:
            attempt += 1
            response = \
                datadotworld.get("https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables?size=1000".
                             format(collection), headers=headers)
            print(response.url, "[", attempt, "]")
            d = loads(response.text)
            df_d = pd.DataFrame(d['records'])
            break
        except (SSLError, ConnectionError, TimeoutError, KeyError, NewConnectionError):
            continue
    catalog_records = pd.concat([df_d, catalog_records], ignore_index=True)

catalog_IRI = catalog_records.explode('collections')
catalog_IRI = catalog_IRI[['id', 'encodedIri', 'collections']]
catalog_IRI = pd.concat([catalog_IRI.drop(['collections'], axis=1),
                         catalog_IRI['collections'].apply(pd.Series)], axis=1)

# Gather catalog records of column metadata using unique table names, which then gives column IRI for eventual url link
column_data = pd.DataFrame()

for i, row in df_tables.iterrows():
    sourceid = row['Source']
    attempt = 0
    while True:
        try:
            attempt += 1
            response = datadotworld.get(
                "https://api.data.world/v0/metadata/data/sources/alleghenycounty/{}/tables/{}/columns?size=100".
                format(sourceid,
                       row['Datatable_Title_value']),
                headers=headers)
            print(response.url, "[", attempt, "]")
            d = loads(response.text)
            df_d = pd.json_normalize(d, 'records')
            break
        except (SSLError, ConnectionError, KeyError, NewConnectionError):
            continue
        except TimeoutError:
            time.sleep(1)
            continue

    column_data = pd.concat([df_d, column_data], ignore_index=True)
    while "nextPageToken" in response.json():
        next_page = d['nextPageToken']
        while True:
            try:
                response = datadotworld.get("""https://api.data.world/v0/{}""".format(next_page), headers=headers)
                print("Next page add: ", next_page)
                d = loads(response.text)
                df_d = pd.json_normalize(d, 'records')
                break
            except (SSLError, ConnectionError, KeyError, NewConnectionError):
                continue
            except TimeoutError:
                time.sleep(1)
                continue
        column_data = pd.concat([df_d, column_data], ignore_index=True)
    time.sleep(0.1)

# column_IRI = column_data.explode('collections')
# column_IRI = column_IRI[['id', 'encodedIri', 'collections']]
# column_IRI = pd.concat([column_IRI.drop(['collections'], axis=1),
#                         column_IRI['collections'].apply(pd.Series)], axis=1)
# column_data = column_data.join(column_IRI["collectionId"], lsuffix="", rsuffix="_x")
column_data = column_data.explode('collections')
column_data['Collection'] = [d.get('collectionId') for d in column_data.collections]

# Merge unique table names and data steward info with catalog records (brings in data table IRI)
df_tables_n = df_tables.merge(catalog_IRI, left_on=['Datatable_Title_value', 'CollectionName_value'],
                              right_on=['id', 'collectionId'])

# Produce counts of columns per table to use
column_data_filtered = column_data.merge(df[['Datatable_Title_value', 'CollectionName_value', 'ColumnTitle_value']],
                                         left_on=['table.tableId', 'Collection', 'title'],
                                         right_on=['Datatable_Title_value', 'CollectionName_value',
                                                   'ColumnTitle_value'],
                                         how="inner")
table_column_count_full = column_data_filtered.groupby(["table.tableId", "CollectionName_value"], as_index=False).size()
table_column_count = table_column_count_full[table_column_count_full['size'] < 5]

# Left join Table/Column counts with column title, collection, and column IRI
column_list = table_column_count.merge(column_data_filtered[['title', 'table.tableId', 'CollectionName_value', 'encodedIri']],
                                       how='left', left_on=['table.tableId', 'CollectionName_value'],
                                       right_on=['table.tableId', 'CollectionName_value'])

# Merge (many to one) unique table names, collection name, and data steward to linked column data
column_list = column_list.merge(df_tables_n[['Datatable_Title_value', 'CollectionName_value', 'DataSteward_value']],
                                left_on=['table.tableId', 'CollectionName_value'],
                                right_on=['Datatable_Title_value', 'CollectionName_value'],
                                validate="many_to_one")

# Produce unique list of data stewards with in data warehouse import, and format address to lower case
stewards_table = df_tables_n[['DataSteward_value', 'DataSteward_EMAIL_value']].copy()
stewards_table['DataSteward_EMAIL_value'] = stewards_table['DataSteward_EMAIL_value'].apply(str.lower)
stewards_table = stewards_table.drop_duplicates()
# USED FOR TESTING, COMMENT/DELETE
# stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus', 'Justin Wier',
#                                                                           'Ali Greenholt', 'Geoffrey Arnold'])]
if dev == "YES":
    stewards_table = stewards_table[stewards_table['DataSteward_value'].isin(['Daniel Andrus'])]

# Opening the html file
HTMLFile = open("""{}/{}""".format(image_subfolder, email_filename), "r")
EmailTemplate = HTMLFile.read()


# Function for creating email message using a loop of all tables and columns associated with a steward,
# then returning an email message based on a provided template. Templates must have empty curly brackets within an
# unordered list html wrapper (<ul>{}</ul>) that designates where bullets will be placed
#
# Required tables not in parameter:
# - table_column_count
# - column_list


def message_creater(stewardess, tables, template):
    link_rows = tables[tables['DataSteward_value'] == stewardess]
    if len(link_rows.index) < 30:
        subcol_list = link_rows.merge(table_column_count[['table.tableId', 'CollectionName_value', 'size']], how='left',
                                      left_on=['Datatable_Title_value', 'CollectionName_value'],
                                      right_on=['table.tableId', 'CollectionName_value'])

        subcol_list_filter = subcol_list[(subcol_list['size'].notnull()) | (subcol_list['size'] > 0)]
        if not subcol_list_filter.empty:
            subcol_list_filter = subcol_list_filter.reset_index(drop=True)
            row_html = []
            if len(subcol_list_filter.index) < 20:
                iterrow_html = []
                for row in subcol_list_filter.index:
                    sub_bullets_list = subcol_list_filter.merge(column_data_filtered,
                                                                left_on=["CollectionName_value",
                                                                         "Datatable_Title_value"],
                                                                right_on=["CollectionName_value",
                                                                          "table.tableId"])
                    sub_bullets_list = sub_bullets_list.merge(
                        df[['CollectionName_value', 'ColumnTitle_value', 'Datatable_Title_value']],
                        left_on=["CollectionName_value", "id_y", "table.tableId_y"],
                        right_on=['CollectionName_value', 'ColumnTitle_value', 'Datatable_Title_value'],
                        how="inner")
                    row_table = subcol_list_filter.iloc[row]['Datatable_Title_value']
                    sub_bullets_list = sub_bullets_list.loc[sub_bullets_list['Datatable_Title_value_x'] == row_table]
                    sub_bullets_list = sub_bullets_list.reset_index(drop=True)
                    sub_bullet_html = []
                    for sub in sub_bullets_list.index:
                        sub_bullet_html.append(
                            """<li><a href="https://data.world/alleghenycounty/catalog/resource/{}">{}</a></li>""".
                            format(sub_bullets_list.iloc[sub]['encodedIri_y'],
                                   sub_bullets_list.iloc[sub]['title_y']))
                    sub_bullet_html = "".join(sub_bullet_html)
                    sub_bullet = """<ul style="padding-left: 30px;type: square;">{}</ul>""".format(sub_bullet_html)
                    iterrow_html.append(
                        """<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>{}""".
                        format(subcol_list_filter.iloc[row]['encodedIri'],
                               subcol_list_filter.iloc[row]['Datatable_Title_value'],
                               sub_bullet))
                row_html = "".join(iterrow_html)
                outer = link_rows.merge(subcol_list_filter, how='outer', indicator=True,
                                        left_on=["CollectionName_value", "Datatable_Title_value"],
                                        right_on=["CollectionName_value", "Datatable_Title_value"],
                                        suffixes=("", "_x"))
                link_rows = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
            else:
                row_html = ""
        else:
            row_html = ""
        link_list = ["""<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>""".
                     format(link_rows['encodedIri'][row],
                            link_rows['Datatable_Title_value'][row]) for row in link_rows.index]
        link_list = "".join(link_list)
        final_list = """{}{}""".format(row_html, link_list)
        final_list = "".join(final_list)
        link_list = final_list
    else:
        link_list = ["""<li><a href="https://data.world/alleghenycounty/catalog/resource/{}/columns">{}</a></li>""".
                     format(link_rows['encodedIri'][row],
                            link_rows['Datatable_Title_value'][row]) for row in link_rows.index]
        link_list = "".join(link_list)

    message = template.format(link_list)
    return message


# Loops through every steward in stewards_table, converts specified tables/columns into links to data catalog,
# creates an email message based on a html template, then emails message to data steward.
image_attach = '{}/{}'.format(image_subfolder, "EditRecord_resize.png")
metadata = MetaData(schema='Reporting')
SQL_stewardTable = Table(
    'ALCO_ddw_StewardsEmailed_Table',
    metadata,
    autoload_with=engine
)
session = Session(engine)

for steward in stewards_table['DataSteward_value']:
    check_existing = SQL_stewardTable.select().where(SQL_stewardTable.c.DataSteward_value == steward)
    check_existing_result = session.execute(check_existing).fetchall()
    if len(check_existing_result) == 0:
        Email_Message = message_creater(steward, df_tables_n, EmailTemplate)
        Steward_Email = \
            stewards_table.loc[stewards_table['DataSteward_value'] == steward, 'DataSteward_EMAIL_value'].values[0]
        send_email(subject=email_subject, to_emails=Steward_Email,
                   message=Email_Message, attachment=image_attach)
        stmt = insert(SQL_stewardTable).values(DataSteward_value=steward,
                                               DataSteward_EMAIL_value=Steward_Email)
        print(stmt)
        with engine.connect() as conn:
            result_add = conn.execute(stmt)
            conn.commit()
            conn.close()
        print(result_add)
