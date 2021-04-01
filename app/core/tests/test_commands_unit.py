import logging
from unittest.mock import patch

import pandas as pd
from django.core.management import call_command
from django.db.utils import OperationalError
from django.test import SimpleTestCase, tag

from core.management.commands.extract_source_metadata import \
    add_publisher_to_source
from core.management.commands.transform_source_metadata import \
    create_target_metadata_dict
from core.management.commands.validate_source_metadata import (
    get_required_fields_for_source_validation,
    source_metadata_value_for_validation)
from core.management.commands.validate_target_metadata import \
    get_required_recommended_fields_for_target_validation

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class CommandTests(SimpleTestCase):
    # globally accessible data sets

    source_metadata = {
        "Duration": 1,
        "Instance": 12345,
        "CatalogURL": "https://test_data.com",
        "DeliveryMode": "test_data",
        "SOURCESYSTEM": "JKO",
        "LearningResourceName": "test_data",
        "LearningResourceIdentifier": "test_data",
        "LearningResourceDescription": "test_data"
    }

    target_metadata = {
        "Course": {
            "CourseCode": "test_data",
            "CourseType": "test_data",
            "CourseTitle": "test_data",
            "CourseDescription": "test_data",
            "CourseProviderName": "JKO",
            "EstimatedCompletionTime": 1
        },
        "CourseInstance": {
            "CourseURL": "https://test_data.com"
        }
    }

    schema_data_dict = {
        'SOURCESYSTEM': 'Required',
        'LearningResourceIdentifier': 'Required',
        'Instance': 'Optional',
        'DeliveryMode': 'Optional',
        'LearningResourceName': 'Required',
        'LearningResourceDescription': 'Required',
        'Duration': 'Optional',
        'CatalogURL': 'Optional'
    }

    target_data_dict = {
        'Course': {
            'CourseProviderName': 'Required',
            'DepartmentName': 'Optional',
            'CourseCode': 'Required',
            'CourseTitle': 'Required',
            'CourseDescription': 'Required',
            'CourseShortDescription': 'Required',
            'CourseFullDescription': 'Optional',
            'CourseAudience': 'Optional',
            'CourseSectionDeliveryMode': 'Optional',
            'CourseObjective': 'Optional',
            'CoursePrerequisites': 'Optional',
            'EstimatedCompletionTime': 'Optional',
            'CourseSpecialNotes': 'Optional',
            'CourseAdditionalInformation': 'Optional',
            'CourseURL': 'Optional',
            'CourseLevel': 'Optional',
            'CourseSubjectMatter': 'Required'
        },
        'CourseInstance': {
            'CourseCode': 'Required',
            'CourseTitle': 'Required',
            'Thumbnail': 'Recommended',
            'CourseShortDescription': 'Optional',
            'CourseFullDescription': 'Optional',
            'CourseURL': 'Optional',
            'StartDate': 'Required',
            'EndDate': 'Required',
            'EnrollmentStartDate': 'Optional',
            'EnrollmentEndDate': 'Optional',
            'DeliveryMode': 'Required',
            'InLanguage': 'Optional',
            'Instructor': 'Required',
            'Duration': 'Optional',
            'CourseLearningOutcome': 'Optional',
            'CourseLevel': 'Optional',
            'InstructorBio': 'Optional'
        },
        'General_Information': {
            'StartDate': 'Required',
            'EndDate': 'Required'
        },
        'Technical_Information': {
            'Thumbnail': 'Recommended'
        }
    }

    # Test cases for waitdb
    def test_wait_for_db_ready(self):
        """Test that waiting for db when db is available"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.return_value = True
            call_command('waitdb')
            self.assertEqual(gi.call_count, 1)

    @patch('time.sleep', return_value=True)
    def test_wait_for_db(self, ts):
        """Test waiting for db"""
        with patch('django.db.utils.ConnectionHandler.__getitem__') as gi:
            gi.return_value = gi
            gi.ensure_connection.side_effect = [OperationalError] * 5 + [True]
            call_command('waitdb')
            self.assertEqual(gi.ensure_connection.call_count, 6)

    # Test cases for extract_source_metadata

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        test_df = pd.DataFrame.from_dict(test_data)
        result = add_publisher_to_source(test_df, 'dau')
        key_exist = 'SOURCESYSTEM' in result[0]
        self.assertTrue(key_exist)

    # Test cases for validate_source_metadata

    def test_get_required_fields_for_source_validation(self):
        """Test for Creating list of fields which are Required """

        result_fields = get_required_fields_for_source_validation(
            self.schema_data_dict)

        self.assertTrue(result_fields)

    @patch('core.management.utils.xia_internal.check_list',
           return_value='List')
    @patch('core.management.utils.xia_internal.check_dict',
           return_value='Dict')
    @patch('core.management.utils.xia_internal.check_validation_value',
           return_value='String')
    def test_source_metadata_value_for_validation(self, mock_list, mock_dict,
                                                  mock_validation_check):
        """Test function to navigate to value in source
        metadata to be validated"""
        test_data_dict = {"key1": "value1",
                          "key2": {"sub_key1": "sub_value1"},
                          "key3": [{"sub_key2": "sub_value2"},
                                   {"sub_key3": "sub_value3"}]}
        return_value = source_metadata_value_for_validation(1, test_data_dict,
                                                            ['key1'],
                                                            'Y')
        self.assertTrue(return_value)

    # Test cases for transform_source_metadata

    def test_create_target_metadata_dict(self):
        """Test to check transformation of source to target schema and
        replacing None values with empty strings"""
        expected_data_dict = {0: self.target_metadata}

        result_data_dict = create_target_metadata_dict(self.target_metadata,
                                                       self.source_metadata)
        self.assertEqual(result_data_dict[0]['Course'].get('CourseCode'),
                         expected_data_dict[0]['Course'].get('CourseCode'))
        self.assertEqual(
            result_data_dict[0]['Course'].get('CourseProviderName'),
            expected_data_dict[0]['Course'].get('CourseProviderName'))

    # Test cases for validate_target_metadata

    def test_get_required_recommended_fields_for_target_validation(self):
        """Test for Creating list of fields which are Required & Recommended"""

        required_dict, recommended_dict = \
            get_required_recommended_fields_for_target_validation(
                self.target_data_dict)

        self.assertTrue(required_dict)
        self.assertTrue(self.target_data_dict)
