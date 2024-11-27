# Inter Warehouse Transfer
This script will take a table from the DataWarehouse or any other version and transfer it to one of the other databases.

Image Name: `countystats/inter-warehouse-transfer:r`

## Enviornmental Variables:
* DEPT: Department Name*
* TABLES: Comma separated string containing any Datawarehouse tables to transfer*
* SCHEMA: Schema from the table you are transfering from
  * Default: Reporting
* SCHEMA_B: Target schema
  * Default: SCHEMA value
* MAX_COLS: Comma separated string containing any MAX length columns in the target table(s)
* WHA_HOST: Warehouse A Connection value*
* WHA_DB: Warehouse A Connection value*
* WHA_UN: Warehouse A Connection value*
* WHA_PW: Warehouse A Connection value*
* WHB_HOST: Warehouse B Connection value*
* WHB_DB: Warehouse B Connection value*
* WHB_UN: Warehouse B Connection value*
* WHB_PW: Warehouse B Connection value*
  
(*) Required variable

## Dag Example
```
wh_connection = BaseHook.get_connection("data_warehouse")
geo_connection = BaseHook.get_connection("GeoSpatialDataWarehouse")
...
transfer_geo = DockerOperator(
        task_id='transfer_geo',
        image='countystats/inter-warehouse-transfer:r',
        api_version='1.39',
        auto_remove=True,
        execution_timeout=timedelta(minutes=20),
        environment={
            'DEPT': dept,
            'SOURCE': onbase_connection.schema,
            'TABLES': 'AsbestosPermits,AsbestosPermits_G',
            'SCHEMA': 'Master',
            'WHA_USER': wh_connection.login,
            'WHA_PASS': wh_connection.password,
            'WHA_HOST': wh_connection.host,
            'WHA_DB': wh_connection.schema,
            'WHB_HOST': geo_connection.host,
            'WHB_DB': geo_connection.schema,
            'WHB_USER': geo_connection.login,
            'WHB_PASS': geo_connection.password
        },
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge"

    )
```
