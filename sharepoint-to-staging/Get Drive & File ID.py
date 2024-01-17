from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import sys
from dotenv import load_dotenv

load_dotenv()

os.chdir(sys.path[0])

pd.set_option('display.max_colwidth', 1000)

# Fill out information to correctly search for the file you want:

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")
drive_type = 'sites' # drive for user (other for sharepoint sites)
drive_name = 'Budget - CountyStat Data Exchange' # Use Sharepoint display name
file_name = 'ARPA Budget File.xlsx'
parsed_name = urllib.parse.quote(file_name)

auth = HTTPBasicAuth(client_id, client_secret)
client = BackendApplicationClient(client_id = client_id)
oauth = OAuth2Session(client=client)

token = oauth.fetch_token(token_url='https://login.microsoftonline.com/e0273d12-e4cb-4eb1-9f70-8bba16fb968d/oauth2/v2.0/token',
                          scope = 'https://graph.microsoft.com/.default',
                          auth = auth,
                          verify = False)

bearer = "Bearer {}".format(token['access_token'])

# Search for drive and get list of visible files

if drive_type == 'drives':
    # Get User Drive ID
    url = "https://graph.microsoft.com/v1.0/users/{}/drive".format(drive_name)
    payload = ""
    headers = {'authorization': bearer}
    response = requests.request("GET", url, data=payload, headers=headers)
    drive = response.json()

    drive_id = drive['id']
    # Search Files
    url = "https://graph.microsoft.com/v1.0/drives/{}/search(q=%27{}%27)".format(drive_id, parsed_name)
    search_r = requests.request("GET", url, data=payload, headers=headers)
    j = search_r.json()['value']
else:
    url = "https://graph.microsoft.com/v1.0/sites?search={}".format(drive_name)
    payload = ""
    headers = {'authorization': bearer}
    response = requests.request("GET", url, data=payload, headers=headers)
    drive = response.json()

    long = drive['value'][0]['id']
    site_id = long.split(',')[1]

    # Get Sharepoint Site Drive ID from DIT
    url = "https://graph.microsoft.com/v1.0/sites/{}/drive/search(q=%27{}%27)".format(site_id, parsed_name)

    search_r = requests.request("GET", url, data=payload, headers=headers)

    j = search_r.json()['value']

# ID for User/Site Drive
if drive_type == 'drives':
    print(drive_id)
else:
    print(site_id)

# Filters Data and shows the most recent files with matching name. Use the value in the id column if this is the correct file
results = pd.DataFrame.from_dict(j)

results = results.sort_values(by = 'createdDateTime', ascending=False).reset_index(drop=True)
filt = results[results["name"].str.contains(file_name)]
filt.head(50)

# If for some reason the filtered dataset does not contain the file you need use the unfiltered list of files
results.head(10)
