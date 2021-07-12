import logging

import requests

from core.models import XISConfiguration

logger = logging.getLogger('dict_config_logger')


def get_xis_metadata_api_endpoint(renamed_data):
    """Retrieve xis metadata api endpoint from XIS configuration """
    logger.debug("Retrieve XIS metadata ledger api endpoint from "
                 "XIS configuration")
    xis_data = XISConfiguration.objects.first()
    xis_metadata_api_endpoint = xis_data.xis_metadata_api_endpoint
    xis_response = response_from_xis(renamed_data, xis_metadata_api_endpoint)
    return xis_response


def get_xis_supplemental_api_endpoint(renamed_data):
    """Retrieve xis supplemental api endpoint from XIS configuration """
    logger.debug("Retrieve XIS supplemental ledger api endpoint from "
                 "XIS configuration")
    xis_data = XISConfiguration.objects.first()
    xis_supplemental_api_endpoint = xis_data.xis_supplemental_api_endpoint
    xis_response = response_from_xis(renamed_data,
                                     xis_supplemental_api_endpoint)
    return xis_response


def response_from_xis(renamed_data, xis_api_endpoint):
    """This function post data to XIS and returns the XIS response to
            XIA load_target_metadata() """
    headers = {'Content-Type': 'application/json'}

    xis_response = requests.post(url=xis_api_endpoint,
                                 data=renamed_data, headers=headers)
    return xis_response
