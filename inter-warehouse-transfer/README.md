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
* WHB_SUFFIX: If specified, appends a suffix on to target table name (1.0 and later tags only)
* WHA_TRUSTED / WHB_TRUSTED: `Yes` connects to that warehouse with a trusted (Kerberos) connection instead of UN/PW (1.1 tag only)
  * Default: No
  * Requires mounting the Airflow Kerberos ticket cache: `Mount(source='/tmp/airflow_krb5_ccache', target='/tmp/krb5cc_0', type='bind', read_only=True)`
* WHB_SQL_BEFORE: SQL run on Warehouse B before any transfer (1.1 tag only)
* WHB_QUERY: Query run on Warehouse B after the transfer; its first row is printed as the final log line, which the DockerOperator pushes to XCom (1.1 tag only)
  * TABLES may be left empty with 1.1 to only run WHB_SQL_BEFORE / WHB_QUERY; Warehouse A variables are then not needed
  
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

## Trusted Connection Example (1.1)
```
transfer_gis = DockerOperator(
        task_id='transfer_gis',
        image='countystats/inter-warehouse-transfer:1.1',
        api_version=Variable.get("docker_api_version"),
        auto_remove='force',
        environment={
            'DEPT': dept,
            'SOURCE': source,
            'TABLES': 'GisPollingPlaces',
            'SCHEMA': 'Staging',
            'SCHEMA_B': 'dbo',
            'WHB_SUFFIX': 'stage',
            'WHA_HOST': wh_connection.host,
            'WHA_DB': wh_connection.schema,
            'WHA_USER': wh_connection.login,
            'WHA_PASS': wh_connection.password,
            'WHB_HOST': gis_connection.host,
            'WHB_DB': gis_connection.schema,
            'WHB_TRUSTED': 'Yes'
        },
        mounts=[Mount(source='/tmp/airflow_krb5_ccache', target='/tmp/krb5cc_0', type='bind', read_only=True)],
        docker_url='unix://var/run/docker.sock',
        network_mode="bridge"
    )
```
