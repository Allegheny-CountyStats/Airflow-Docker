#  staging-to-warehouse

Provide a list of tables within a Staging schema and move them to the Master Datawarehouse schema. All enviornmental variables in the example are required.

Image name: `countystats/staging-to-warehouse:r`

* Overwrite command: `Rscript staging_to_warehouse.R'`
* Append Command: `Rscript staging_append_warehouse.R`
  * Can specify which tables must produce new rows with 'REQ_TABLES' variable
* Replace/Append Command: `Rscript staging_replace_warehouse.R`
  * Can specify which tables must produce new rows with 'REQ_TABLES' variable

## Example Dag Usages:

#### Overwrite Example:
```
connection = BaseHook.get_connection("mssql_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
...
staging_to_warehouse = DockerOperator(
                task_id='staging_to_warehouse',
                image='countystats/staging-to-warehouse:r',
                api_version='1.39',
                auto_remove='force',
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
                network_mode="bridge"
        )
```

#### Replace/Append Example:
```
connection = BaseHook.get_connection("mssql_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
...
staging_to_warehouse = DockerOperator(
                task_id='staging_to_warehouse',
                image='countystats/staging-to-warehouse:r',
                api_version='1.39',
                auto_remove='force',
                environment={
                    'DEPT': 'Department_Name_and/or_Warehouse_Schema',
                    'TABLE': 'TableName' OR 'TABLES': 'Comma,Separated,Table,Names',
                    'REQ_TABLES': 'Tables, Names, Comma,Separated', # New rows required, must exist in TABLES variable
                    'ID_COL': 'id_col', #ID_COLS must have the same name if doing multiple tables
                    'CALC_UID': 'NO', #Indicates whether ID_COLS is a calculated field: if so, excldues from insert statement, available in 'staging-to-warehouse:calc_col'
                    'SOURCE': connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript staging_replace_warehouse.R',
                network_mode="bridge"
        )
```
