import pendulum
from datetime import time
from airflow.sdk import task
from airflow.models import Variable


@task.short_circuit(task_id='tableau_check')
def tableau_check(**_):
    now = pendulum.now()  # Airflow-safe timezone-aware timestamp

    # Check: Is it the third Tuesday of the month?
    # weekday(): Monday=0 ... Sunday=6 → Tuesday=1
    is_tuesday = (now.weekday() == 1)
    is_third_week = 15 <= now.day <= 21

    # Check: Is it between 20:00 and 23:00?
    start_window = time(20, 0)  # 8 PM
    end_window = time(23, 0)  # 11 PM
    in_time_window = start_window <= now.time() <= end_window

    if is_tuesday and is_third_week and in_time_window:
        return False
    else:
        return True


@task.short_circuit(task_id="dev_ShortCircuit")
def dev_test():
    if Variable.get('DEV_POWER_SWITCH') == "TRUE":
        host_check = True
    else:
        host_check = False
    return host_check
