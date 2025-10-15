from airflow.providers.slack.notifications.slack import SlackNotifier

# Slack Alerts
SLACK_CONNECTION_ID = "slack"
SLACK_CHANNEL = "airflow"


def slack_airflow_notification(**context):
    slack_msg = """
                :red_circle: Task Failed. 
                *Task*: {task}  
                *Dag*: {dag} 
                *Execution Time*: {exec_date}  
                *Log Url*: {log_url} 
                """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    SlackNotifier(
        slack_conn_id=SLACK_CONNECTION_ID,
        text=slack_msg,
        channel=SLACK_CHANNEL,
    )
