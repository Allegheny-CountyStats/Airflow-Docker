# Network to Staging Templates

Templates for transferring files on county network drives to the CountyStat Data Warehouse

### Example Docker Operator
```python
from airflow.hooks.base import BaseHook
from docker.types import Mount
from datetime import timedelta

# BE SURE ABOVE IMPORTS INCLUDED IN DAG IMPORT LINES

wh_connection = BaseHook.get_connection("data_warehouse")

pull_FileOnNetwork = DockerOperator(
        task_id='pull_FileOnNetwork',
        image='countystats/network_to_warehouse:latest',
        api_version='1.39',
        auto_remove=True,
        execution_timeout=timedelta(minutes=5),
        environment={
            'DEPT': "DEPARTMENT NAME",
            'TABLE': 'TABLE NAME IN WAREHOUSE',
            'SOURCE': 'DATA SOURCE',
            'WH_HOST': wh_connection.host,
            'WH_DB': wh_connection.schema,
            'WH_USER': wh_connection.login,
            'WH_PASS': wh_connection.password,
            'SHEET': 'SOME SHEET NAME',
            'WORKBOOK': 'SomeExcelWorkbook.xlsx',
            'FILEPATH': './SomeFilePath', # File path within mounted drive to directory
            'COLTYPES': "date,numeric,date,numeric,text,text,text,text,text" # Comma Seperated List
        },
        docker_url='unix://var/run/docker.sock',
        command='bash network_to_warehouse.sh ',  # MUST HAVE SPACE AT END OF COMMAND, MUST I TELL YOU
        network_mode="bridge",
        mounts=[
            Mount(
                source='/home/mgradmin/Kerberos',
                target='/Kerberos',
                type='bind'
            ),
            Mount(
                source='/media/SOME_FOLDER_NAME',
                target='/SOME_TARGET_DIRECTORY_IN_CONTAINER',
                type='bind'
            )
        ]
    )
```

### Current Templates:
* [excel_to_staging.R](excel_to_staging.R): Transforms an Excel sheet into a table within the Staging schema of CountyStat Warehouse