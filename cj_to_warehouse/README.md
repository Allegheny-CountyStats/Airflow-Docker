# cj-to-warehouse

Provide a list of tables within a Criminal Justice Data Warehouse schema and move them to the normal Data Warehouse. 

Image name: `countystats/cj-to-warehouse:r`

## Enviornmental Variables
* DEPT: Department Name*
* TABLES: Comma seperated list of tables to be pulled from Criminal Justice Data Warehouse source*
* SOURCE: Souce Schema/Database or API description*
* SCHEMA: Schema the tables come from in the Criminal Justice Data Warehouse
  * Default: `Reporting`
* APPEND: `TRUE`/`FALSE` for if the new tables should be appeneded instead of overwritten in the Data Warehouse,
  * Default: `FALSE`
* CJ_USER: Criminal Justice Data Warehouse login Connection value *
* CJ_PASS: Criminal Justice Data Warehouse password Connection value*
* CJ_HOST: Criminal Justice Data Warehouse host Connection value*
* CJ_DB: Criminal Justice Data Warehouse schema Connection value*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*

*Required Variable

## Example Dag Usage:
```
cj_connection = BaseHook.get_connection("cj_data_warehouse")
wh_connection = BaseHook.get_connection("data_warehouse")
...
table_pull = DockerOperator(
                task_id='table_pull',
                image='countystats/cj-to-warehouse:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Department_Name',
                    'TABLES': 'Name,Of,Tables,Comma,Separated',
                    'SOURCE': 'Example_Schema',
                    'CJ_USER': cj_connection.login,
                    'CJ_PASS': cj_connection.password,
                    'CJ_HOST': cj_connection.host,
                    'CJ_DB': cj_connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```

## Push to Criminal Justice from Warehouse:

To perform the opposite data flow (data-warehouse to criminal-justice), flip the connection parameters used in the above example as done below:

```
push_cj = DockerOperator(
        task_id='push_cj',
        image='countystats/cj-to-warehouse:r',
        api_version='1.39',
        auto_remove=True,
        execution_timeout=timedelta(minutes=20),
        environment={
            'DEPT': dept,
            'SOURCE': 'EXAMPLE_SCHEMA',
            'TABLES': 'LIST,OF,TABLES,FROM,DATA,WAREHOUSE,COMMA,SEPERATED',
            'CJ_USER': wh_connection.login,
            'CJ_PASS': wh_connection.password,
            'CJ_HOST': wh_connection.host,
            'CJ_DB': wh_connection.schema,
            'WH_HOST': cj_connection.host,
            'WH_DB': cj_connection.schema,
            'WH_USER': cj_connection.login,
            'WH_PASS': cj_connection.password
        },
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge"
    )
```
