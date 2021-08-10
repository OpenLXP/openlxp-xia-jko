import hashlib
import logging

import pandas as pd
from openlxp_xia.management.utils.xia_internal import get_key_dict

from core.models import XSRConfiguration

logger = logging.getLogger('dict_config_logger')


def read_source_file():
    """setting file path from s3 bucket"""
    xsr_data = XSRConfiguration.objects.first()
    file_name = xsr_data.source_file
    extracted_data = pd.read_excel(file_name, engine='openpyxl')
    std_source_df = extracted_data.where(pd.notnull(extracted_data),
                                         None)
    #  Creating list of dataframes of sources
    source_list = [std_source_df]

    logger.debug("Sending source data in dataframe format for EVTVL")
    # file_name.delete()
    return source_list


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['LearningResourceIdentifier', 'SOURCESYSTEM']
    field_values = []

    for item in field:
        if not data_dict.get(item):
            logger.info('Field name ' + item + ' is missing for '
                                               'key creation')
            return None
        field_values.append(data_dict.get(item))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.md5(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key
