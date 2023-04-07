#!/usr/bin/env python
# coding: utf-8

# In[12]:


import os
import pandas as pd
import sqlalchemy as sa
import pantab 
import tableauserverclient as TSC
import sys
from tableauhyperapi import TableName
from dotenv import load_dotenv

load_dotenv()

# Env Variables
dept = os.getenv('dept')
table = os.getenv('table')
name = os.getenv('name')
mode = os.getenv('mode', 'Overwrite')
project_id = os.getenv('project_id')
project_name = os.getenv('project_name', dept)
site = os.getenv('site', 'CountyStats')

# Load Tableau Credentials
tableau_username = os.getenv("ts_username")
tableau_password = os.getenv("ts_password")

# Load Datawarehouse Credentials
wh_host = os.getenv("wh_host")
wh_db = os.getenv("wh_db")
wh_un = os.getenv("wh_user")
wh_pw = os.getenv("wh_pass")

# Build Connection & Query Warehouse
wh_conn_string = "mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server".format(wh_un, wh_pw, wh_host, wh_db)
engine = sa.create_engine(wh_conn_string)

# Connect to Tableau Server
server = TSC.Server('https://tableau.alleghenycounty.us')
server.version = '3.3'

tableau_auth = TSC.TableauAuth(tableau_username, tableau_password, site_id=site)


# Find if data source exist
with server.auth.sign_in(tableau_auth):
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, name))
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.ProjectName, TSC.RequestOptions.Operator.Equals, project_name))
    all_datasources = server.datasources.get(req_option)
    if str(all_datasources[0]) != '[]':
        first = all_datasources[0][0]
        first_check = str(type(first)) == """<class 'tableauserverclient.models.datasource_item.DatasourceItem'>""" and first._datasource_type == 'hyper'
    else:
        first_check = False

# Download Data
with server.auth.sign_in(tableau_auth):
    server.datasources.download(datasource_id = first.id, include_extract = False)

