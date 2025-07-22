from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

default_message = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """


def task_fail_slack_alert(context, slack_conn_id, slack_message=default_message):
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    slack_msg = slack_message.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='Airflow')
    return failed_alert.execute(context=context)