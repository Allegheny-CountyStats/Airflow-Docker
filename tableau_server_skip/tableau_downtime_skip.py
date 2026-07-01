from datetime import datetime


# Added task for Tableau maintenance on third tuesday 8-11pm
def tableau_skip(**_):
    # Get today's date
    today = datetime.today()
    now = datetime.now()
    weekday = int(today.strftime("%w"))
    day = int(today.strftime("%d"))
    hour = int(now.strftime("%H"))
    # check criteria
    is_tuesday = (weekday == 2)
    is_third_week = (15 <= day <= 21)
    in_time_window = (20 <= hour <= 23)
    #
    if is_tuesday and is_third_week and in_time_window:
        return "skip_tableau"
    else:
        return "tableau_tasks"

# tableau_skip()