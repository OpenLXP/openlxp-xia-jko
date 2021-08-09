import logging
from unittest.mock import patch
import pandas as pd
from ddt import ddt
from django.test import tag

from core.management.utils.xsr_client import (
    read_source_file)

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class UtilsTests(TestSetUp):
    """Unit Test cases for utils """

    # Test cases for XSR_CLIENT
    def test_read_source_file(self):
        """Test to check the extraction of source data from XSR for EVTVL"""
        with patch('core.management.utils.xsr_client'
                   '.XSRConfiguration.objects') as xsrCfg, \
                patch('core.management.utils.xsr_client.'
                      'pd.read_excel')as ext_data:
            xsrCfg.first.source_file.return_value = 'Source_file'
            ext_data.return_value = pd.DataFrame. \
                from_dict(self.source_metadata, orient='index')
            return_from_function = read_source_file()
            self.assertIsInstance(return_from_function, list)
