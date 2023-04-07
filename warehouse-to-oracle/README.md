# warehouse-to-oracle

Write a table to an Oracle Database, primarily to transfer data to DHS.

Image name: `countystats/warehouse-to-oracle:r`

## Enviornmental Variables

* DEPT: Department name*
* TABLES: Source table names*
  * Separated by commas
* SOURCE: Oracle database, schema, to be transferred to*
* USER: Oracle Database login Connection to be transferred to*
* PASSWORD: Oracle Database password Connection to be transferred to*
* HOST: Oracle Database host Connection to be transferred to*
* PORT: Oracle Database port Connection to be transferred to*
  * Default `1521`
* DATABASE: Source Database schema Connection to be transferred to*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*
* WH_SCHEMA: Data Warehouse table(s) schema
  * Default: `Reporting`

*Required Variable

### DAG Example:
```
connection = BaseHook.get_connection("shuman")
wh_connection = BaseHook.get_connection("cj_data_warehouse")
dhs_connection = BaseHook.get_connection("dhsdwprd_datawarehouse")
...
schema = schema_device = DockerOperator(
                task_id='schema_validate',
                image='countystats/warehouse-to-oracle:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': dept,                    
                    'SOURCE': connection.schema,
                    'TABLES': 'DailyPopulation_V',
                    'USER': dhs_connection.login,
                    'PASS': dhs_connection.password,
                    'HOST': dhs_connection.host,
                    'DATABASE': dhs_connection.schema,
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='Rscript schema-validate.R',
                network_mode="bridge"
        )
```
