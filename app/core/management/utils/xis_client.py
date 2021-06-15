import logging

import requests

from core.models import XISConfiguration

logger = logging.getLogger('dict_config_logger')


def get_xis_api_endpoint():
    """Retrieve xis api endpoint from XIS configuration """
    logger.debug("Retrieve xis_api_endpoint from XIS configuration")
    xis_data = XISConfiguration.objects.first()
    xis_api_endpoint = xis_data.xis_api_endpoint
    return xis_api_endpoint


def response_from_xis(renamed_data):
    """This function post data to XIS and returns the XIS response to
            XIA load_target_metadata() """
    headers = {'Content-Type': 'application/json'}

    xis_response = requests.post(url=get_xis_api_endpoint(),
                                 data=renamed_data, headers=headers,
                                 timeout=6.0)
    return xis_response
