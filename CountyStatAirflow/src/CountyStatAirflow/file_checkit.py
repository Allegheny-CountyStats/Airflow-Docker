from moveitAPI.moveit import auth_move_it, available_files
from airflow.sdk import Variable
from airflow.sdk import PokeReturnValue
from airflow.sdk import task
import os
import dotenv
import pandas as pd


@task.sensor(mode="reschedule", poke_interval=43200, timeout=604800)
def check_moveit(file_name, folder_path = None, file_types:list[str] = None):
    condition_met = False
    calls_filename = None
    base_url = 'alleghenycounty.us'
    auth = f"grant_type=password&username={Variable.get('moveit_username')}&password={Variable.get('moveit_password')}"
    moveit_tokens = auth_move_it(base_url, auth)
    moveit_files = available_files(base_url, moveit_tokens)
    if folder_path is None:
        name_k = moveit_files
    else:
        print(f"Checking for folder path: {folder_path}")
        name_k = [d for d in moveit_files if d["path"].startswith(folder_path)]
    if len(name_k) == 1:
        current_files = [d for d in name_k if
                         d["name"].__contains__(file_name)]
        current_month_fil = []
        for file_type in file_types:
            current_month_fil.extend([d for d in current_files if
                                      d["name"].endswith(file_type)])
        if len(current_month_fil) != 0:
            condition_met = True
            calls_filename = name_k[0].get('name')
        else:
            condition_met = False
            calls_filename = None
    elif len(name_k) > 1:
        current_files = [d for d in name_k if
                         d["name"].__contains__(file_name)]
        current_month_fil = []
        if file_types is not None:
            for file_type in file_types:
                current_month_fil.extend([d for d in current_files if
                                          d["name"].endswith(file_type)])
        if len(current_month_fil) == 1:
            condition_met = True
            calls_filename = current_month_fil[0].get('name')
        elif len(current_month_fil) > 1:
            Exception("Files are present, but confused naming convention or repeated uploads")
            condition_met = False
            calls_filename = None
        elif len(current_month_fil) == 0:
            condition_met = False
            calls_filename = None
        else:
            Exception("Issue encountered with trying to count potential matches")
    else:
        condition_met = False
        calls_filename = None
        print(f"Still waiting for file upload")
    return PokeReturnValue(is_done=condition_met, xcom_value=calls_filename)

