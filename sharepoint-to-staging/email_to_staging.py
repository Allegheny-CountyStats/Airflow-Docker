#!/usr/bin/env python
# coding: utf-8
from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import sqlalchemy as sa
import time
from sqlalchemy.engine import URL
import re
from msgraph import GraphServiceClient

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
engine = sa.create_engine(connection_url)

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

url = "https://graph.microsoft.com/v1.0/users/CountyStat@alleghenycounty.us/mailFolders/inbox"

response = requests.request("GET", url, data="", headers=headers)
inbox = response.json()

url = "https://graph.microsoft.com/v1.0/users/CountyStat@alleghenycounty.us/mailFolders/{}/messages?filter=from/emailAddress/address eq 'charles.perry@alleghenycounty.us' and hasAttachments eq true".format(inbox.get("id"))

response = requests.request("GET", url, data="", headers=headers)
messages = response.json()

print("hello")

df.to_sql(name=table_name, schema=schema, con=engine, if_exists='replace', index=False)
