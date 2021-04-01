import hashlib
import logging
from unittest.mock import patch

from django.test import SimpleTestCase, tag

from core.management.utils.xia_internal import (check_validation_value,
                                                get_key_dict,
                                                get_source_metadata_key_value,
                                                get_target_metadata_key_value,
                                                replace_field_on_target_schema)
from core.management.utils.xis_client import get_xis_api_endpoint
from core.management.utils.xss_client import get_aws_bucket_name

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class CommandTests(SimpleTestCase):
    """Unit Test cases for utils """

    # Test cases for XIA_INTERNAL

    def test_get_key_dict(self):
        """Test for key dictionary creation"""
        arg1 = 'test_key'
        arg2 = 'test_key_hash'
        expected_result = {
            'key_value': arg1,
            'key_value_hash': arg2
        }
        result = get_key_dict(arg1, arg2)
        self.assertEqual(result, expected_result)

    def test_get_source_metadata_key_value(self):
        """Test key dictionary creation for source"""
        test_dict = {
            'LearningResourceIdentifier': 'key_field1',
            'SOURCESYSTEM': 'key_field2'
        }

        expected_key = 'key_field1_key_field2'
        expected_key_hash = hashlib.md5(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_source_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)

    def test_replace_field_on_target_schema(self):
        """test to check if values under educational context are replaced"""
        test_dict0 = {'0': {
            "Course": {
                "EducationalContext": "Y"
            }
        }
        }

        test_dict1 = {'1': {
            "Course": {
                "EducationalContext": "n"
            }
        }
        }

        replace_field_on_target_schema('0', test_dict0)
        self.assertEqual(test_dict0['0']['Course']['EducationalContext'],
                         'Mandatory')

        replace_field_on_target_schema('1', test_dict1)
        self.assertEqual(test_dict1['1']['Course']['EducationalContext'],
                         'Non - Mandatory')

    def test_get_target_metadata_key_value(self):
        """Test key dictionary creation for target"""

        test_dict = {'Course': {
            'CourseCode': 'key_field1',
            'CourseProviderName': 'key_field2'
        }}

        expected_key = 'key_field1_key_field2'
        expected_key_hash = hashlib.md5(expected_key.encode('utf-8')). \
            hexdigest()

        result_key_dict = get_target_metadata_key_value(test_dict)
        self.assertEqual(result_key_dict['key_value'], expected_key)
        self.assertEqual(result_key_dict['key_value_hash'], expected_key_hash)

    @patch('core.management.utils.xia_internal.required_recommended_logs',
           return_value=None)
    def test_check_validation_value_Y(self, arg):
        """Test the function which returns the source bucket name"""
        ind = 1
        ele = 'abc'
        prefix = 'test'
        validation_result = 'Y'
        result = check_validation_value(ind, ele, prefix,
                                        validation_result)
        self.assertEqual('Y', result)

    @patch('core.management.utils.xia_internal.required_recommended_logs',
           return_value=None)
    def test_check_validation_value_N(self, arg):
        """Test the function which returns the source bucket name"""
        ind = 1
        ele = ''
        prefix = 'test'
        validation_result = 'Y'
        result = check_validation_value(ind, ele, prefix,
                                        validation_result)
        self.assertEqual('N', result)

    # Test cases for XIS_CLIENT

    def test_get_api_endpoint(self):
        """Test to check if API endpoint is present"""
        result_api_value = get_xis_api_endpoint()
        self.assertTrue(result_api_value)

    # Test cases for XSR_CLIENT

    # Test cases for XSS_CLIENT

    def test_get_aws_bucket_name(self):
        """Test the function which returns the source bucket name"""
        result_bucket = get_aws_bucket_name()
        self.assertTrue(result_bucket)
