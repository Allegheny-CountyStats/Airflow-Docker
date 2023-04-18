# socrata-to-staging

Uses the Socrata API to pull public data and transfer it to the data warehouse.
Image name: `countystats/socrata-to-staging:python`

## Enviornmental Variables
* SCHEMA: Schema where the table will be*
* TABLE: Table Name that will be in warehouse*
* WH_HOST: Data Warehouse login Connection value*
* WH_DB: Data Warehouse database Connection value*
* WH_UN: Data Warehouse login Connection value*
* WH_PW: Data Warehouse password Connection value*

*Required Variable

## Example Dag Usage:
```
connection = BaseHook.get_connection("airflow_connection_id") # This is pulled from Admin > connections
wh_connection = BaseHook.get_connection("data_warehouse")
...
socrata_pull = DockerOperator(
                task_id='socrata_pull',
                image='countystats/socrata-to-staging:python',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'SCHEMA': 'Name of schema in data warehouse',
                    'TABLES': 'Name of table in data warehouse',
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                command='python pull-socrata.py',
                network_mode="bridge"
        )
```
