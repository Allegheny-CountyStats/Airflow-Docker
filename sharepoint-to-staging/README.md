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
* drive_id: ID of the user or Sharepoint groups Drive. See [Finding IDs](#finding-ids).*
* file_id: File ID of the file. See [Finding IDs](#finding-ids).*
* drive_type: Whether the endpoint should call drives (user / any library) or sites (sharepoint group's default library) endpoint.
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

## Finding IDs

`Get Drive & File ID.py` turns the names you see in SharePoint into `drive_type`, `drive_id` and `file_id`. It reads `client_id`/`client_secret` from the environment or the nearest `.env` (this folder or a parent).

```
# Site display name + file name (searches every library on the site)
python "Get Drive & File ID.py" --site "IDE Data Warehouse" --file WWS_Dataset.xlsx

# Site URL copied from the browser (any page on the site works)
python "Get Drive & File ID.py" --site "https://alleghenycounty.sharepoint.com/sites/<Site>" --file WWS_Dataset.xlsx

# Exact lookup by library + folder: use this for newly uploaded/renamed files that search hasn't indexed yet
python "Get Drive & File ID.py" --site "IDE Data Warehouse" --library Documents --path General --file WWS_Dataset.xlsx

# A user's OneDrive
python "Get Drive & File ID.py" --user someone@alleghenycounty.us --file report.xlsx

# Site / library ids only, no file (e.g. drive_id for sharepoint_upload)
python "Get Drive & File ID.py" --site "IDE Data Warehouse" --no-file
```

Leaving out the file (`--no-file`, or `FILE_NAME = None` at the top of the script) skips the file search. The script lists the site's libraries with their drive ids and prints `drive_type`/`drive_id` for the default library, or for the library named with `--library`.

Use `--path /` for the library root, `--all` to list every match, and `--insecure` if TLS verification fails on your network. You can also set the defaults at the top of the script and run it with no arguments.

The script lists matches best-first (exact name, then partial, newest first) with library, folder and link so you can confirm the file, then prints the values for the DAG:
* File in the site's default library (usually "Documents"): `drive_type: 'sites'`, `drive_id` = site id.
* File in another library or a OneDrive: `drive_type: 'drives'`, `drive_id` = that library's drive id. `sharepoint_upload.py` only supports the default library.

If a site name matches several sites, the script stops and lists them; re-run with the site's URL.

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
* drive_id: Site id of the Sharepoint group. See [Finding IDs](#finding-ids) (`sites` mode value).*
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
