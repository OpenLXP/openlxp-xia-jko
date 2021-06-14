import logging

import pandas as pd

from core.models import XIAConfiguration

logger = logging.getLogger('dict_config_logger')


def read_source_file():
    """setting file path from s3 bucket"""
    xia_data = XIAConfiguration.objects.first()
    file_name = xia_data.source_file
    extracted_data = pd.read_excel(file_name, engine='openpyxl')
    std_source_df = extracted_data.where(pd.notnull(extracted_data),
                                         None)
    #  Creating list of dataframes of sources
    source_list = [std_source_df]

    logger.debug("Sending source data in dataframe format for EVTVL")
    file_name.delete()
    return source_list
