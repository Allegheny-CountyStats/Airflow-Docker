from airflow.models import Variable


def dev_test():
    if Variable.get('DEV_POWER_SWITCH') == "TRUE":
        host_check = True
    return host_check