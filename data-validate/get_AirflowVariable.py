import requests
import json
import pandas as pd
import os

from dotenv import load_dotenv
load_dotenv()

airflow_user = os.getenv("AIRFLOW_USER")
airflow_password = os.getenv("AIRFLOW_PASSWORD")

payload = {
    "username": airflow_user,
    "password": airflow_password
  }
header_auth = {
    "Content-Type": "application/json"
}

r2 = requests.post(
    url="https://devairflow.alleghenycounty.us:8080/auth/token",
    headers=header_auth,
    data=json.dumps(payload)
)
return_call = json.loads(r2.content)
api_token = return_call.get("access_token")

# Get Variable
get_auth = {
    "Authorization": f"Bearer {api_token}"
}
r4 = requests.get(
    url="https://devairflow.alleghenycounty.us:8080/api/v2/variables/ACCELA_API_Password",
    headers=get_auth
)
