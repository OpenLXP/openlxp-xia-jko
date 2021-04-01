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
    file_name.delete()
    return std_source_df
