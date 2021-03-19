import logging
import os
import requests

logger = logging.getLogger('dict_config_logger')


def get_api_endpoint():
    """Setting API endpoint from XIA and XIS communication """
    api_endpoint = os.environ.get('API_ENDPOINT')
    return api_endpoint


def response_from_xis(renamed_data):
    """This function post data to XIS and returns the XIS response to
            XIA load_target_metadata() """
    headers = {'Content-Type': 'application/json'}

    xis_response = requests.post(url=get_api_endpoint(),
                                 data=renamed_data, headers=headers,
                                 timeout=6.0)
    return xis_response
