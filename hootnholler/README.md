Package installed at dev within /home/mgradmin/.local/lib/python3.10/site-packages
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

def callback_fail(context):
    task_fail_slack_alert(slack_conn_id=SLACK_CONN_ID, slack_message=error_message, context=context)
    hoot_update(state='sad', sentry='SentryId', ddw_bearer=Variable.get("ddw_sa_token"),
                cookie_setting=Variable.get("ddw_sentry_cookie"), ddw_history_note="Sent from testingHoot.py")


default_args = { # replace within default args
    'on_failure_callback': callback_fail
}   
```
