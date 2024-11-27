Package installed at dev within /home/mgradmin/.local/lib/python3.10/site-packages

    from hootnholler.slack_alert import task_fail_slack_alert
    from hootnholler.hoot_alert import hoot_update    
    
    def callback_fail(context):
            task_fail_slack_alert(slack_conn_id=SLACK_CONN_ID, slack_message=error_message, context=context)
    
    
    default_args = {
        'owner': 'Airflow',
        'description': 'Hoot Tester',
        'depend_on_past': False,
        'start_date': datetime(2021, 4, 28, 0, 0, tzinfo=local_tz),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': callback_fail,
        'max_active_runs': 1
    }


Edited the profile to add ssl variables for python requests:
```
nano ~/.profile
#add lines to file
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
# write out lines
# exit nano editor
source ~/.profile
```