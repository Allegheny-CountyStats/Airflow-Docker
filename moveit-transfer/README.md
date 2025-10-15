#  Instructions

Provide a tables in the Datawarehouse to transfer to the County's Move It SFTP server.

The command argument for this file is different defending on the action you are taking. For Uploads use: `Rscript moveit-transfer.R` for downloads use: `Rscript moveit-download.R`

Image Name: `countystats/moveit-transfer:2.0` or `countystats/moveit-transfer:r` or `countystats/moveit-transfer:3.0`

## Enviornmental Variables:

* DEPT: Department and schema in Datawarehouse*
* TABLE: Table name for transfer to/from in Datawarehouse*
* FILENAME: 
  * transfer: File name to upload to move it server
    * Default: Table name ending with `.csv`
  * download: File name to download off the move it server§
* FILE_ID: 
  * download: ID of the file on the move it server
* FOLDER: Folder to upload the file to†
* FOLDER_PATH: For instances of a folder existing in multiple locations users. Only available in version `2.0`
* ROWNAMES: 
  * upload: Include rownames in csv
  * Default: `FALSE`
* MAX_COLS: Only available in version `2.0`
  * download: Names of the columns which should have a max character length and appended to the end of the table.
      * Putting `'auto'` will make the task automatically determine which columns should be max character length.
* SNAKECASE 
  * download: Whether or not to change the column names to snake case.
  * Default: `FALSE`
* MI_USER: Airflow Variable for move it server user*
* MI_PASS: Airflow Variable for move it server password*
* BASE_URL: Base URL for move it server
  * Default: `alleghenycounty.us`
* SA_USER: Airflow Connection value - Deprecated but still functions
* SA_PASS: Airflow Connection value - Deprecated but still functions
* WH_HOST: Airflow Connection value*
* WH_DB: Airflow Connection value*

(*) required both

(†) required for `movite-transfer.R`

(§) required for `movite-download.R`


## Example Dag Usage:

### Upload
```
connection = BaseHook.get_connection("mssql_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
moveit_folder = 'Some Folder'
...
# 2.0 (with Max Character Cols)
...
mi_device = DockerOperator(
                task_id='mi_device',
                image='countystats/moveit-transfer:2.0',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': dept,
                    'TABLE': 'Some_Table',
                    'FILENAME': "Example.csv",
                    'FOLDER': moveit_folder,
                    'MAX_COLS': 'some_column_name,another_column',
                    'DATABASE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'SA_USER': wh_connection.login,
                    'SA_PASS': wh_connection.password,
                    'MI_USER': Variable.get('moveit_username'),
                    'MI_PASS': Variable.get('moveit_password')
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript moveit-transfer.R',
                network_mode="bridge"
        )
# Old Version
mi_device = DockerOperator(
                task_id='mi_device',
                image='countystats/moveit-transfer:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': dept,
                    'TABLE': 'Some_Table',
                    'FILENAME': "Example.csv",
                    'FOLDER': moveit_folder,
                    'DATABASE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'SA_USER': wh_connection.login,
                    'SA_PASS': wh_connection.password,
                    'MI_USER': Variable.get('moveit_username'),
                    'MI_PASS': Variable.get('moveit_password')
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript moveit-transfer.R',
                network_mode="bridge"
        )
```

### Download

```
wh_connection = BaseHook.get_connection("data_warehouse")
source = 'Some Source'
...
pull_example = DockerOperator(
                task_id='pull_example',
                image='countystats/moveit-transfer:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': dept,
                    'MI_USER': Variable.get('moveit_username'),
                    'MI_PASS': Variable.get('moveit_password'),
                    'SOURCE': source,
                    'FILENAME': 'example.csv',
                    'SNAKECASE': 'TRUE',
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript moveit-download.R',
                network_mode="bridge"
        )
```
