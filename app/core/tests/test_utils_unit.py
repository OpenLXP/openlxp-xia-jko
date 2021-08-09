import hashlib
import logging
from unittest.mock import patch

import pandas as pd
from ddt import data, ddt, unpack
from django.test import tag

from core.management.utils.xsr_client import (get_source_metadata_key_value,
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

    @data(('key_field1', 'key_field2'), ('key_field11', 'key_field22'))
    @unpack
    def test_get_source_metadata_key_value(self, first_value, second_value):
        """Test key dictionary creation for source"""
        test_dict = {
            'LearningResourceIdentifier': first_value,
            'SOURCESYSTEM': second_value
        }

        expected_key = first_value + '_' + second_value
        expected_key_hash = hashlib.md5(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_source_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)
