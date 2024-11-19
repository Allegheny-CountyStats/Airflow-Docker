import requests
from requests.models import PreparedRequest


def hoot_update(state='happy', sentry=None, ddw_bearer=None, ddw_user_message='', ddw_history_note='',
                cookie_setting=''):
    url = "https://data.world/h/api/v1/sentries/{}?".format(sentry)

    if sentry is None:
        raise TypeError('No Sentry Supplied for Hoot Alert/Update')

    if ddw_bearer is None:
        raise TypeError('No Bearer Token Supplied for Data.World API Call')

    payload = {}
    params = {'state': state, 'user_message': ddw_user_message, 'history_note': ddw_history_note, 'private_entry': 'True'}
    req = PreparedRequest()
    req.prepare_url(url, params)
    headers = {
        'Authorization': ddw_bearer,
        'Cookie': cookie_setting
    }

    response = requests.request("POST", req.url, headers=headers, data=payload)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return "Error: " + str(e)
