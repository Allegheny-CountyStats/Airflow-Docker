# Documentation on Kerberos Authentication within CountyStats

## [RECOMMENDED] Utilizing R-Basic:4.2.1

An updated r-basic image (tagged 4.2.1) includes kerberos packages necessary for authentication. It requires mounting a 
folder in Airflow server to the DAG task (/home/mgradmin/Kerberos/). This folder contains a keytab file with credentials
for the CountyStats service account. 

Example staging to warehouse task with mount:
```python
from docker.types import Mount

staging_to_warehouse = DockerOperator(
                task_id='staging_to_warehouse',
                image='countystats/staging-to-warehouse:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Department_Name',
                    'TABLES': 'Name,Of,Tables,Comma,Separated',
                    'REQ_TABLES': 'Tables, Names, Comma,Separated', # New rows required, must exist in TABLES variable
                    'ID_COL': 'id_col', #ID_COLS must have the same name if doing multiple tables
                    'SOURCE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript staging_to_warehouse.R',
                network_mode="bridge",
                mounts=[
                    Mount(
                        source='/home/mgradmin/Kerberos',
                        target='/Kerberos',
                        type='bind'
                    )
        ]
        )
```

Within ETL script connecting to CountyStat Warehouse, the following R commands can be employed:
```r
system(kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t Kerberos/sa00427.keytab)

wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass, Trusted_Connection = "Yes")
```

Python alternative:
```python
import os

cmd = "kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t Kerberos/sa00427.keytab"
os.system(cmd)


```

## Adding Kerberos Authentication to Image built with Dockerfile [DO NOT PUSH IMAGE TO DOCKERHUB]

Steps:
- Write Keytab File to image folder using ktutil (WriteKeytab.sh) from Putty
  - Can include multiple users from Allegheny.County.Local domain 
  - Requires interactive bash session to enter password for account listed in keytab (below code uses T115235 AND sa00427 as examples):
```shell
ktutil
addent -password -p T115235@COUNTY.ALLEGHENY.LOCAL -k 1 -e rc4-hmac
addent -password -p sa00427@COUNTY.ALLEGHENY.LOCAL -k 1 -e rc4-hmac
wkt sa00427.keytab
exit
```
- Enusre dockerfile references image with Kerberos packages installed or add these packages to RUN command lines of the dockerfile:
```dockerfile
RUN apt-get update && \
    DEBIAN_FRONTEND='noninteractive' apt-get install -y --no-install-recommends \
    ca-certificates\
    realmd\
    libpam-sss\
    libnss-sss\
    sssd\
    sssd-tools\
    adcli\
    krb5-user\
    libpam-krb5
```
- Using the filename of the keytab file created in the first step, insert the following lines to Dockerfile of project/image:
```dockerfile
COPY sa00427.keytab /
RUN kinit sa00427@COUNTY.ALLEGHENY.LOCAL -k -t sa00427.keytab
```
- This configuration allows for subsequent connections to use a 'Trusted Connection', like this example R script:
```r
wh_con <- dbConnect(odbc::odbc(), driver = "{ODBC Driver 17 for SQL Server}", server = wh_host, database = wh_db, UID = wh_user, pwd = wh_pass, Trusted_Connection = "Yes")
```
