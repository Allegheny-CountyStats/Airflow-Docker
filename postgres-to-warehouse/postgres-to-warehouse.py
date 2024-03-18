import json
import os
import pandas as pd
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
import sqlalchemy as sa
from sqlalchemy import delete, Table, MetaData, insert, select

connection_url = "postgresql://mgradmin:airflow_password@host.docker.internal/airflow"
engine = sa.create_engine(connection_url)
metadata = MetaData(schema='public')

ImportTable = Table(
    'dag',
    metadata,
    autoload_with=engine
)