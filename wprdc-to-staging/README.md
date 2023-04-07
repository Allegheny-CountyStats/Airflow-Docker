# WPRDC-to-staging

Provide a list of tables within a database schema and move them to the Datawarehouse from an IBM WRPDC Database. 

Image name: `countystats/wprdc-to-staging:r`

* sql_statement: Custom SQL code for pulling out data from the API.
  * Note: if you are using this feature then you shouldn't pass the resource_code var.
* resource_code: Resource code for the source you're pulling.
* table_name: Table name to write to including the department source and table.
* wh_host: Data Warehouse login Connection value*
* wh_db: Data Warehouse database Connection value*
* wh_un: Data Warehouse login Connection value*
* wh_pw: Data Warehouse password Connection value*


## Example:
```
connection = BaseHook.get_connection("WRPDC_connection_id")
wh_connection = BaseHook.get_connection("data_warehouse")
Dept = 'ExampleDept
source = 'Example'
...
pull_example = DockerOperator(
                task_id='pull_example,
                image='countystats/wprdc-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'resource_code': "66cdcd57-6c92-4aaa-8800-0ed9d8f03e22",
                    'table_name': f"{dept}_{source}_{table}",
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )

pull_example = DockerOperator(
                task_id='pull_example,
                image='countystats/WRPDC-to-staging:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'sql_statement': """SELECT COUNT("_id") AS "NumberofPropertySales", date_part(\'year\', "SALEDATE") AS "Year" 
                    FROM "5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1" 
                    WHERE "SALEDATE" >= \'2014-01-01\' GROUP BY date_part(\'year\', "SALEDATE")
                    """,
                    'table_name': f"{dept}_{source}_{table}",
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )        
```
