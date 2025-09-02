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
import json
from io import BytesIO

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
source = os.getenv('source', 'Email')
table = os.getenv('table')
table_name = '{}_{}_{}'.format(dept, source, table)

# Email Information
mailbox_id = os.getenv('mail_id', None)
message_filter = os.getenv('message_filter')
sort_by = os.getenv('sort_by', None)
top_num = os.getenv('top_by', None)

# Auth creds
bearer = os.getenv('bearer_token', None)
auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)

if bearer is None:
    # Auth token
    token = oauth.fetch_token(
        token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
        scope='https://graph.microsoft.com/.default',
        auth=auth)

    bearer = "Bearer {}".format(token['access_token'])
headers = {'authorization': bearer}


# Get Request Function
def get_request(url_name, headers_list):
    try:
        response = requests.request("GET", url_name, data="", headers=headers_list)
        response.raise_for_status()  # Raises HTTPError for bad status codes (4xx or 5xx)
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    else:
        print("Request was successful.")
        return response.json()


# Inbox
if mailbox_id is None:
    url = "https://graph.microsoft.com/v1.0/users/CountyStat@alleghenycounty.us/mailFolders/inbox"
    inbox = get_request(url, headers)
    inbox_name = inbox.get("id")

else:
    inbox_name = mailbox_id

# Message
if message_filter is not None:
    filter_text = "filter=" + message_filter
else:
    filter_text = None

if sort_by is not None:
    sort_text = "orderby=" + sort_by
else:
    sort_text = None

if top_num is not None:
    top_text = "top=" + top_num
else:
    top_text = None


def combine_strings(*args, sep=" "):
    return sep.join(str(arg) for arg in args if arg is not None)


post_txt = combine_strings(filter_text, sort_text, top_text, sep="&")

url = "https://graph.microsoft.com/v1.0/users/CountyStat@alleghenycounty.us/mailFolders/{}/messages?{}".format(
        inbox_name, post_txt)
messages = get_request(url, headers)

att_url = "https://graph.microsoft.com/v1.0/users/CountyStat@alleghenycounty.us/mailFolders/{}/messages/{}/attachments".format(inbox_name, messages.get("value").__getitem__(0).get("id"))
attachment = get_request(att_url, headers)

att_raw_url = "{}/{}/$value".format(att_url, attachment.get("value").__getitem__(0).get("id"))
attachment_raw = requests.request("GET", att_raw_url, data="", headers=headers)

if filetype == 'csv':
    csv_file = BytesIO(attachment_raw.content)
    df = pd.read_csv(csv_file)

if filetype != 'csv':
    if filetype == 'x':
        filename = 'test.xlsx'
    else:
        filename = 'test.xls'
    with open(filename, 'wb') as output:
        output.write(attachment_raw.content)

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

if filetype == 'text':
    doit = 1+2
# print(df.head())

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


df.to_sql(name=table_name, schema=schema, con=engine, if_exists='replace', index=False)
