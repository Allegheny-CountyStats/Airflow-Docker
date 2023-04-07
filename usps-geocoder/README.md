# usps-geocoder

This script will take a table from the Datawarehouse and geocode addresses.

Image Name: `countystats/usps-geocode:r`

## Enviornmental Variables:
* DEPT: Schema and Department Name*
* TABLE: Datawarehouse table name*
* SOURCE: Database or data source
* ID_COL: Unique identifier column*
* FULL_ADDRESS: SQL code to create a `FULL_ADDRESS` column from the dataset.*
* WHERE: IF set to true the query will use the ID column to ensure only new rows are geocoded and append to the existing table. If set to any other value the entire table with be geocded and the existing table will be overwritten.
  * Default: `TRUE`
* WH_HOST: Airflow Connection value*
* WH_DB: Airflow Connection value*
* WH_UN: Airflow Connection value*
* WH_PW: Airflow Connection value*

(*) Required variable

## Dag Example

```
wh_connection = BaseHook.get_connection("data_warehouse")
connection = BaseHook.get_connection("example")
...
geocode = DockerOperator(
                task_id='geocode',
                image='countystats/usps-geocode:r',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'DEPT': 'Example_Dept',
                    'TABLE': 'Example',
                    'SOURCE': connection.schema,
                    'ID_COL': 'ID',
                    'WHERE': 'TRUE',
                    'WH_HOST': wh_connection.host,
                    'WH_DB': wh_connection.schema,
                    'WH_USER': wh_connection.login,
                    'WH_PASS': wh_connection.password
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```
