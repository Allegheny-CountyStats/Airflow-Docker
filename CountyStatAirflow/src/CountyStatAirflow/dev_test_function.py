from airflow.models import Variable


def dev_test():
    if Variable.get('DEV_POWER_SWITCH') == "TRUE":
        host_check = True
    else:
        host_check = False
    return host_check
