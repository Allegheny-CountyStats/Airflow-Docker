# sharepoint-to-staging

This script will take data from Outlook email messages (attachments) and import them in the Data Warehouse.

Image Name: `countystats/outlook-to-staging:1.0`

## Enviornmental Variables:
* dept: Schema and Department Name*
* table: Datawarehouse table name*
* schema: Datawarehouse Schema to pull from
  * Default: `Staging`
* sheet: If not loading the first sheet of the Excel document, put complete sheet name here
  * Default: First sheet of Excel document
* client_id: Microsoft Graph API Airflow Variable*
* client_secret: Microsoft Graph API Airflow Variable*
* wh_host: Airflow Connection value*
* wh_db: Airflow Connection value*
* wh_un: Airflow Connection value*
* wh_pw: Airflow Connection value*
* snakecase: Format column names in snake case
  * Default: "NO"

(*) Required variable

## Dag Example

```
wh_connection = BaseHook.get_connection("data_warehouse")
dept = 'Example_Dept'
...
pull_motions = DockerOperator(
                task_id='pull_motions',
                image='countystats/sharepoint-to-staging:python',
                api_version='1.39',
                auto_remove=True,
                environment={
                    'wh_host': wh_connection.host,
                    'wh_db': wh_connection.schema,
                    'wh_user': wh_connection.login,
                    'wh_pass': wh_connection.password,
                    'client_id': Variable.get("o365_client_id"),
                    'client_secret':  Variable.get("o365_client_secret"),
                    'dept': dept,
                    'table': 'ExampleTable',
                    'drive_id': 'some_id_from_notebook',
                    'file_id': 'some_other_id_from_notebook'
                },
                docker_url='unix://var/run/docker.sock',
                network_mode="bridge"
        )
```

