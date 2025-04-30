from oauthlib.oauth2 import BackendApplicationClient
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
import requests
import json
import urllib.parse
import pandas as pd
import os
import time
import re
import json
from io import BytesIO

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

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
        print("Request was successful.")
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if mailbox_id is None: # if none provided, defaults to CountyStat inbox
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
final_dict = messages["value"]
while '@odata.nextLink' in messages.keys():
    new_url = messages.get('@odata.nextLink')
    messages = get_request(new_url, headers)
    final_dict = final_dict + messages["value"]
print(final_dict)
