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
        data = {
            "LMS": ["Success Factors LMS v. 5953"],
            "XAPI": ["Y"],
            "SCORM": ["N"]}

        test_df = pd.DataFrame.from_dict(data)
        result = add_publisher_to_source(test_df, 'dau')
        key_exist = 'SOURCESYSTEM' in result[0]
        self.assertTrue(key_exist)

    # Test cases for validate_source_metadata

    def test_get_required_fields_for_source_validation(self):
        """Test for Creating list of fields which are Required """

        schema_data_dict = {
            'SOURCESYSTEM': 'Required',
            'crs_id': 'Optional',
            'crs_header': 'Required',
            'crs_name': 'Required',
            'crs_description': 'Required',
            'crs_objective': 'Optional',
            'crs_attendies': 'Optional',
            'cat_images': 'Optional',
            'cat_fy': 'Optional',
            'temp1': 'Optional',
            'sord_id': 'Optional',
            'crs_url': 'Optional',
            'Start_date': 'Required',
            'End_date': 'Required',
            'CEU': 'Optional',
            'CLP': 'Optional',
            'RRP': 'Optional',
            'IsCurrent': 'Optional'
        }
        result_fields = get_required_fields_for_source_validation(
            schema_data_dict)

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
        source_data_dict = {
            "CEU": "0",
            "CLP": "1",
            "RRP": "0",
            "CF_ID": "",
            "temp1": "9/27/2018",
            "cat_fy": "2008",
            "crs_id": "2146",
            "Crs_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "crs_mode": "Continuous Learning",
            "crs_name": "test data",
            "IsCurrent": "true",
            "crs_notes": "test data",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "crs_header": "FAC 066",
            "crs_layout": "N/A",
            "crs_length": "Approximately 1 hour",
            "crs_pdscode": "ZZD",
            "SOURCESYSTEM": "DAU",
            "crs_attendies": "test data",
            "crs_objective": "",
            "crs_pagenumber": "0",
            "crs_postscript": "N/A",
            "crs_description": "test data",
            "crs_prerequisite": "None",
            "cat_chapternumber": "0"
        }
        target_mapping_dict = {
            "Course": {
                "CourseCode": "ACQ 3700",
                "CourseTitle": "Acquisition Law",
                "CourseAudience": "test_data",
                "DepartmentName": "",
                "CourseObjective": "test_data",
                "CourseDescription": "test_data",
                "CourseProviderName": "DAU",
                "CourseSpecialNotes": "test_data",
                "CoursePrerequisites": "None",
                "EstimatedCompletionTime": "4.5 days",
                "CourseSectionDeliveryMode": "Resident",
                "CourseAdditionalInformation": "None"
            },
            "CourseInstance": {
                "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
            },
            "General_Information": {
                "EndDate": "end_date",
                "StartDate": "start_date"
            }
        }
        expected_data_dict = {
            0: {
                "Course": {
                    "CourseCode": "ACQ 3700",
                    "CourseTitle": "Acquisition Law",
                    "CourseAudience": "test_data",
                    "DepartmentName": "",
                    "CourseObjective": "test_data",
                    "CourseDescription": "test_data",
                    "CourseProviderName": "DAU",
                    "CourseSpecialNotes": "test_data",
                    "CoursePrerequisites": "None",
                    "EstimatedCompletionTime": "4.5 days",
                    "CourseSectionDeliveryMode": "Resident",
                    "CourseAdditionalInformation": "None"
                },
                "CourseInstance": {
                    "CourseURL": "https://dau.tes.com/ui/lms-learning-details"
                },
                "General_Information": {
                    "EndDate": "end_date",
                    "StartDate": "start_date"
                }
            }
        }
        result_data_dict = create_target_metadata_dict(target_mapping_dict,
                                                       source_data_dict)
        self.assertEqual(result_data_dict[0]['Course'].get('CourseCode'),
                         expected_data_dict[0]['Course'].get('CourseCode'))
        self.assertEqual(
            result_data_dict[0]['Course'].get('CourseSectionDeliveryMode'),
            expected_data_dict[0]['Course'].get('CourseSectionDeliveryMode'))

    # Test cases for validate_target_metadata

    def test_get_required_recommended_fields_for_target_validation(self):
        """Test for Creating list of fields which are Required & Recommended"""

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
        required_dict, recommended_dict = \
            get_required_recommended_fields_for_target_validation(
                target_data_dict)

        self.assertTrue(required_dict)
        self.assertTrue(target_data_dict)
