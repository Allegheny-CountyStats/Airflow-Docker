#  staging-to-warehouse

Provide a list of tables within a Staging schema and move them to the Master Datawarehouse schema. All enviornmental variables in the example are required.

Image name: `countystats/staging-to-warehouse:r`

* Overwrite command: `Rscript staging_to_warehouse.R'`
* Append Command: `Rscript staging_append_warehouse.R`
  * Can specify which tables must produce new rows with 'REQ_TABLES' variable
* Replace/Append Command: `Rscript staging_replace_warehouse.R`
  * Can specify which tables must produce new rows with 'REQ_TABLES' variable

Tags:
- r
- health
- calc_col
- 2.0
- 2.1

### 2.1: safe replace
`staging_replace_warehouse.R` in the `2.1` tag won't leave Master missing rows when the insert fails:
* Before deleting anything, it compares every char/varchar/nchar/nvarchar column that Staging declares wider than Master against Master's limit. If any Staging value is too long, the task stops and lists each column, the Master limit, the longest Staging value and how many rows are over. Nothing is deleted.
* For each table, the delete and the insert run in a single transaction. If the insert fails for any reason, the delete is rolled back, Master stays unchanged and the Staging table is kept, so you can re-run.
* Staging tables are dropped only after every table succeeds.
* Takes `ID_COLS` (comma separated, for joining on more than one column). Falls back to `ID_COL` if `ID_COLS` isn't set, so DAGs on the `r`/`health` tags can switch by changing only the image tag. `CALC_UID` from `calc_col` isn't supported.


## Example Dag Usages:

#### Overwrite Example:
```
connection = BaseHook.get_connection("mssql_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
...
staging_to_warehouse = DockerOperator(
                task_id='staging_to_warehouse',
                image='countystats/staging-to-warehouse:r',
                api_version=Variable.get("docker_api_version"),
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
                api_version=Variable.get("docker_api_version"),
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
