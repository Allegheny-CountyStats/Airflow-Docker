from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Slack Alerts
SLACK_CONN_ID = 'slack'


def task_fail_slack_alert(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {logical_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username='Airflow')
    return failed_alert.execute(context=context)
