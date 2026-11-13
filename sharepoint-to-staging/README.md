# sharepoint-to-staging

This script will take data from Individual Sharepoints/Office 365 Excel Documents and placing them in the Data Warehouse.

Image Name: `countystats/sharepoint-to-staging:python`

## Tags
* `2.0`: `to_sql(if_exists='replace')` with batched inserts, retried when SQL Server picks it as a deadlock victim.
* `2.1`: **load-then-swap**. Parallel loads into the same database no longer deadlock:
  1. an empty private table `<table>__load_<random>` is created and committed;
  2. rows are inserted into it, a transaction that locks only that private table;
  3. the old staging table is dropped and the private one renamed into place (`sp_rename`) in one short transaction.

  The staging table appears complete or not at all. A failed run drops its private table. Leftovers, if any, match `%\_\_load\_%`. Table names and column types are the same as 2.0.

## Enviornmental Variables:
* dept: Schema and Department Name*
* table: Datawarehouse table name*
* schema: Datawarehouse Schema to pull from
  * Default: `Staging`
* sheet: If not loading the first sheet of the Excel document, put complete sheet name here
  * Default: First sheet of Excel document
* client_id: Microsoft Graph API Airflow Variable*
* client_secret: Microsoft Graph API Airflow Variable*
* drive_id: ID of the user or Sharepoint groups Drive. Use `Get Drive & File ID.ipynb` to get ID.*
* file_id: File ID of the file. Use `Get Drive & File ID.ipynb` to get ID.*
* drive_type: Whether the endpoint should call drives (user) or sites (sharepoint group) endpoint.
  * Default: `drives` (user)
* wh_host: Airflow Connection value*
* wh_db: Airflow Connection value*
* wh_un: Airflow Connection value*
* wh_pw: Airflow Connection value*
* snakecase: Format column names in snake case
  * Default: "NO"
* deadlock_retries: (2.0/2.1) Attempts when SQL Server picks a write as a deadlock victim (error 1205); waits a random 5-20s x attempt between tries. In 2.1 this covers only the short create and swap steps
  * Default: 5

(*) Required variable

## Dag Example

```
wh_connection = BaseHook.get_connection("data_warehouse")
dept = 'Example_Dept'
...
pull_motions = DockerOperator(
                task_id='pull_motions',
                image='countystats/sharepoint-to-staging:python',
                api_version=Variable.get("docker_api_version"),
                auto_remove='force',
                environment={
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password,
                    'client_id': Variable.get("o365_client_id"),
                    'client_secret':  Variable.get("o365_client_secret"),
                    'dept': dept,
                    'table': 'ExampleTable',
                    'drive_id': 'some_id_from_notebook',
                    'file_id': 'some_other_id_from_notebook'
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge",
                mounts=[
                    Mount(
                        source='/usr/local/share/ca-certificates/',
                        target='/usr/local/share/ca-certificates/',
                        type='bind',
                        read_only=True
                    ),
                    Mount(
                        source='/etc/ssl/certs/',
                        target='/etc/ssl/certs/',
                        type='bind',
                        read_only=True
                    )
                ]
        )
```

# sharepoint_upload (excel file)

Template to load file from K: into a sharepoint site folder.

Image Name: `countystats/sharepoint-to-staging:1.0`

Needs the following command within the docker operator:
`command= 'python3 sharepoint_upload.py'`

## Environmental Variables:
* client_id: Microsoft Graph API Airflow Variable*
* client_secret: Microsoft Graph API Airflow Variable*
* drive_id: ID of the user or Sharepoint groups Drive. Use `Get Drive & File ID.ipynb` to get ID.*
* filename: name of file to upload*
* folder_name: name of target sharepoint folder, defaults to `None` which lands the file in the root Documents folder.

(*) Required variable

## Dag Example

```
...
upload_motions = DockerOperator(
                task_id='upload_motions',
                image='countystats/sharepoint-to-staging:1.0',
                api_version=Variable.get("docker_api_version"),
                auto_remove=True,
                environment={
                    'client_id': Variable.get("o365_client_id"),
                    'client_secret':  Variable.get("o365_client_secret"),
                    'drive_id': 'some_id_from_notebook',
                    'filename': 'some_filename',
                    'source_folder_name': 'some_folder_name_on_MountedDrive',
                    'target_folder_name': 'some_folder_name_on_sharepoint'
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge",
                command= 'python3 sharepoint_upload.py',
                mounts=[
                        Mount(
                            source='/usr/local/share/ca-certificates/',
                            target='/usr/local/share/ca-certificates/',
                            type='bind',
                            read_only=True
                        ),
                        Mount(
                            source='/etc/ssl/certs/',
                            target='/etc/ssl/certs/',
                            type='bind',
                            read_only=True
                        )
                ]
        )
```
