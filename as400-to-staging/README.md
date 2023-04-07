# as400-to-staging

Provide a list of tables within a database schema and move them to the Datawarehouse from an IBM AS400 Database. 

Image name: `countystats/as400-to-staging:r`

* dept: Department Name*
* table: Table to be pulled from Data source*
* sql: Custom SQL code for pulling out a table.
  * Note: if you are using this feature then you should only pass one table name in the `TABLES` field.
* uid: Source Database login Connection value *
* pwd: Source Database password Connection value*
* host: Source Database host Connection value*
* source: Source name*
* wh_host: Data Warehouse login Connection value*
* wh_db: Data Warehouse database Connection value*
* wh_un: Data Warehouse login Connection value*
* wh_pw: Data Warehouse password Connection value*
* append_col: Column to check for new values
* date_col: Name of column needed to transform date from Julian Calendar
  * Default: `append_col`
* append_type: SQL Function to use to find new values
  * Default value `MAX`
* append_sign: WHERE statement sign for appending new data.
  * Default: `>`

## Example:
```
connection = BaseHook.get_connection("as400_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
Dept = 'ExampleDept
source = 'Example'
...
pull_example = DockerOperator(
                task_id='pull_example,
                image='countystats/as400-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'dept': dept,
                    'host': connection.host,
                    'uid': connection.login,
                    'pwd': connection.password,
                    'source': source,
                    'table': 'Example',
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```
