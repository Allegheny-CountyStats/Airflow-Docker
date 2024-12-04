# DataDotWorld Hoots/Sentry Usage within Airflow

Utilizes custom 'hootnholler' package, stored within airflow python library at: **`/home/mgradmin/.local/lib/python3.10/site-packages`**

Two methods outlined below:
* On Task Failure (airflow task callback)
* On Tableau Push Success

## Airflow Task Callback (Failure or Success)

```python
from hootnholler.slack_alert import task_fail_slack_alert
from hootnholler.hoot_alert import hoot_update    
from airflow.models import Variable

# Slack variables
error_message = """
        :white_circle: Hoot Test. 
        *Task*: {task}  
        *Dag*: {dag} 
        *Execution Time*: {exec_date}  
        *Log Url*: {log_url} 
        """
SLACK_CONN_ID = 'slack'

def callback_fail(context):  # Also includes slack_fail notification
    task_fail_slack_alert(slack_conn_id=SLACK_CONN_ID, slack_message=error_message, context=context)
    hoot_update(state='sad', sentry='SentryId', ddw_bearer=Variable.get("ddw_sa_token"),
                cookie_setting=Variable.get("ddw_sentry_cookie"), ddw_history_note="Sent from testingHoot.py")


def callback_success(context):
    hoot_update(state='happy', sentry='SentryId', ddw_bearer=Variable.get("ddw_sa_token"),
                cookie_setting=Variable.get("ddw_sentry_cookie"), ddw_history_note="Sent from testingHoot.py")
    

default_args = { # replace within default args
    'on_failure_callback': callback_fail,
    'on_success_callback': callback_success
}
```
# Tableau Push Success
Only configured for task success: for failures, use the task-failure callback shown above. 

```python
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

wh_connection = BaseHook.get_connection("data_warehouse")

tableau_demog = DockerOperator(
        task_id='tableau_demog',
        image='countystats/tableau-transfer:1.0',
        api_version='1.39',
        auto_remove=True,
        environment={
            'name': '##_$$',
            'table': '##_$$',
            'ts_username': Variable.get("tableau_username"),
            'ts_password': Variable.get("tableau_password"),
            'wh_host': wh_connection.host,
            'wh_db': wh_connection.schema,
            'wh_user': wh_connection.login,
            'wh_pass': wh_connection.password,
            'project_name': '##_$$',
            'HOOT_SENTRY': '##_$$',  # Required 
            'HOOT_TOKEN': Variable.get("ddw_sa_token"),  # Required
            'HOOT_COOKIE': Variable.get("ddw_sentry_cookie"),
            'HOOT_USER_MESSAGE': '##_$$',
            'HOOT_HISTORY_NOTE': '##_$$'
        },
        docker_url='unix://var/run/docker.sock',
        command='python3 Tableau-Transfer.py',
        network_mode="bridge"
    )
```
