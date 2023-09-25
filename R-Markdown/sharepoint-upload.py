from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import re
import calendar
from datetime import date, timedelta, datetime

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
sp_folder_name = os.getenv("folder_name", None)  # Folder required
if sp_folder_name is None:
    sp_folder_name = os.getenv("dept", None)
drive = os.getenv("drive")
ds = datetime.strptime(os.getenv('ds'), '%Y-%m-%d')
filename = os.getenv("filename")

# Create Auth object
auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id=client_secret)
oauth = OAuth2Session(client=client)
# Obtain Graph API Token
token = oauth.fetch_token(token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
                      scope='https://graph.microsoft.com/.default',
                      verify=False,
                      auth=auth)
bearer = "Bearer {}".format(token['access_token'])
# Build headers
payload = ""
headers = {'authorization': bearer}
# Find the root folder to upload to
root_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/root".format(drive)
root_r = requests.request("GET", root_url, data=payload, headers=headers, verify=False)
root = root_r.json()['id']

items_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(drive, root)
items_r = requests.request("GET", items_url, data=payload, headers=headers)
children = pd.DataFrame.from_dict(items_r.json()['value'])
folder_id = children[children['name'] == sp_folder_name].iloc[0]['id']
# Upload the file to the correct folder location
put_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}:/{}:/content".format(drive, folder_id, filename)
upload_headers = headers = {'authorization': bearer, 'Content-Type': 'application/json'}
upload = requests.request("PUT", put_url, data = open(new_name, 'rb'), headers=headers)
try:
    upload.raise_for_status()
except requests.exceptions.HTTPError as err:
    print(err)
