from uuid import UUID

import pandas as pd
from django.test import TestCase


class TestSetUp(TestCase):
    """Class with setup and teardown for tests in XIS"""

    def setUp(self):
        """Function to set up necessary data for testing"""

        # globally accessible data sets

        self.source_metadata = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "test name",
            "Start_date": "2017-03-28T00:00:00-04:00",
            "LearningResourceIdentifier": "TestData 123",
            "SOURCESYSTEM": "JKO",
            "test_description": "test description",
            "supplemental_data": "sample1"
        }

        self.key_value = "TestData 123_JKO"
        self.key_value_hash = "0a453b6bea6e7b1d25fb9799ef734f57"
        self.hash_value = "f454114ba41034e14df2a8f3c14a047d"

        self.target_metadata = {
            "Course": {
                "CourseCode": "TestData 123",
                "CourseTitle": "Acquisition Law",
                "CourseAudience": "test_data",
                "DepartmentName": "",
                "CourseObjective": "test_data",
                "CourseDescription": "test description",
                "CourseProviderName": "JKO",
                "CourseSpecialNotes": "test_data",
                "CoursePrerequisites": "None",
                "EstimatedCompletionTime": "4.5 days",
                "CourseSectionDeliveryMode": "Resident",
                "CourseAdditionalInformation": "None"
            },
            "CourseInstance": {
                "CourseURL": "https://jko.tes.com/ui/lms-learning-details"
            },
            "General_Information": {
                "EndDate": "end_date",
                "StartDate": "start_date"
            }
        }

        self.target_key_value = "TestData 123_JKO"
        self.target_key_value_hash = "0a453b6bea6e7b1d25fb9799ef734f57"
        self.target_hash_value = "eaf3e57b7f21b4d813f1258fb4ebf89d"
        self.schema_data_dict = {
            'SOURCESYSTEM': 'Required',
            'test_id': 'Optional',
            'LearningResourceIdentifier': 'Required',
            'test_name': 'Required',
            'test_description': 'Required',
            'test_objective': 'Optional',
            'test_attendies': 'Optional',
            'test_images': 'Optional',
            'test1_id': 'Optional',
            'test_url': 'Optional',
            'Start_date': 'Required',
            'End_date': 'Required',
            'Test_current': 'Recommended'
        }

        self.target_data_dict = {
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

        self.xia_data = {
            'metadata_record_uuid': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'target_metadata': {
                "Course": {
                    "CourseCode": "TestData 123",
                    "CourseTitle": "Acquisition Law",
                    "CourseAudience": "test_data",
                    "DepartmentName": "",
                    "CourseObjective": "test_data",
                    "CourseDescription": "test_data",
                    "CourseProviderName": "JKO",
                    "CourseSpecialNotes": "test_data",
                    "CoursePrerequisites": "None",
                    "EstimatedCompletionTime": "4.5 days",
                    "CourseSectionDeliveryMode": "Resident",
                    "CourseAdditionalInformation": "None"
                },
                "CourseInstance": {
                    "CourseURL": "https://jko.tes.com/ui/lms-learning-details"
                },
                "General_Information": {
                    "EndDate": "end_date",
                    "StartDate": "start_date"
                }
            },
            'target_metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'target_metadata_key': 'TestData 123_JKO',
            'target_metadata_key_hash': '6acf7689ea81a1f792e7668a23b1acf5'
        }

        self.xis_expected_data = {
            'unique_record_identifier': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'metadata': {
                "Course": {
                    "CourseCode": "TestData 123",
                    "CourseTitle": "Acquisition Law",
                    "CourseAudience": "test_data",
                    "DepartmentName": "",
                    "CourseObjective": "test_data",
                    "CourseDescription": "test_data",
                    "CourseProviderName": "JKO",
                    "CourseSpecialNotes": "test_data",
                    "CoursePrerequisites": "None",
                    "EstimatedCompletionTime": "4.5 days",
                    "CourseSectionDeliveryMode": "Resident",
                    "CourseAdditionalInformation": "None"
                },
                "CourseInstance": {
                    "CourseURL": "https://jko.tes.com/ui/lms-learning-details"
                },
                "General_Information": {
                    "EndDate": "end_date",
                    "StartDate": "start_date"
                }
            },
            'metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'metadata_key': 'TestData 123_JKO',
            'metadata_key_hash': '6acf7689ea81a1f792e7668a23b1acf5',
            'provider_name': 'JKO'
        }

        self.xia_supplemental_data = {
            'metadata_record_uuid': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'supplemental_metadata': {
                "Field1": "ABC",
                "Field2": "123",
                "Field3": "ABC-123"
            },
            'supplemental_metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'supplemental_metadata_key': 'TestData 123_JKO',
            'supplemental_metadata_key_hash':
                '6acf7689ea81a1f792e7668a23b1acf5'
        }

        self.xis_supplemental_expected_data = {
            'unique_record_identifier': UUID(
                '09edea0e-6c83-40a6-951e-2acee3e99502'),
            'metadata': {
                "Field1": "ABC",
                "Field2": "123",
                "Field3": "ABC-123"
            },
            'metadata_hash': 'df0b51d7b45ca29682e930d236963584',
            'metadata_key': 'TestData 123_JKO',
            'metadata_key_hash':
                '6acf7689ea81a1f792e7668a23b1acf5',
            'provider_name': 'JKO'
        }
        self.source_target_mapping = {
            "Course": {
                "CourseProviderName": "SOURCESYSTEM",
                "DepartmentName": "",
                "CourseCode": "LearningResourceIdentifier",
                "CourseTitle": "test_name",
                "CourseDescription": "test_description",
                "CourseAudience": "test_attendies",
                "CourseSectionDeliveryMode": "test_mode",
                "CourseObjective": "test_objective",
                "CoursePrerequisites": "test_prerequisite",
                "EstimatedCompletionTime": "test_length",
                "CourseSpecialNotes": "test_notes",
                "CourseAdditionalInformation": "test_postscript"
            },
            "CourseInstance": {
                "CourseURL": "test_url"
            },
            "General_Information": {
                "StartDate": "start_date",
                "EndDate": "end_date"
            }
        }

        self.test_metadata_column_list = [["Test", "Test_id", "Test_url"]]

        self.source_metadata_with_supplemental = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "supplemental_data1": "sample1",
            "supplemental_data2": "sample2"
        }

        self.supplemental_data = {
            "supplemental_data1": "sample1",
            "supplemental_data2": "sample2"
        }

        self.metadata_invalid = {
            "Test": "0",
            "Test_id": "2146",
            "Test_url": "https://example.test.com/",
            "End_date": "9999-12-31T00:00:00-05:00",
            "test_name": "",
            "Start_date": "",
            "LearningResourceIdentifier": "TestData 1234",
            "SOURCESYSTEM": "JKO",
            "test_description": "test description",
        }

        self.key_value_invalid = "TestData 1234_JKO"
        self.key_value_hash_invalid = "eaf3e57b7f21b4d813f1258fb4ebf89d"
        self.hash_value_invalid = "0a453b6bea6e7b1d25fb9799ef734f57"

        self.target_metadata_invalid = {
            "Course": {
                "CourseCode": "TestData 1234",
                "CourseTitle": "Acquisition Law",
                "CourseAudience": "test_data",
                "DepartmentName": "",
                "CourseObjective": "test_data",
                "CourseDescription": "",
                "CourseProviderName": "JKO",
                "CourseSpecialNotes": "test_data",
                "CoursePrerequisites": "None",
                "EstimatedCompletionTime": "4.5 days",
                "CourseSectionDeliveryMode": "Resident",
                "CourseAdditionalInformation": "None"
            },
            "CourseInstance": {
                "CourseURL": "https://jko.tes.com/ui/lms-learning-details"
            },
            "General_Information": {
                "EndDate": "end_date",
                "StartDate": "start_date"
            }
        }
        self.target_key_value_invalid = "TestData 1234_JKO"
        self.target_key_value_hash_invalid = "eaf3e57b7f21b4d813f1258fb4ebf89d"
        self.target_hash_value_invalid = "0a453b6bea6e7b1d25fb9799ef734f57"

        self.test_required_column_names = ['SOURCESYSTEM',
                                           'LearningResourceIdentifier',
                                           'Start_date', 'End_date']
        self.test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        self.test_data1 = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        self.xis_api_endpoint_url = 'http://openlxp-xis:8020/api/metadata/'
        self.supplemental_api_endpoint = 'http://openlxp-xis:8020' \
                                         '/api/supplemental-data/'

        self.receive_email_list = ['receiver1@openlxp.com',
                                   'receiver1@openlxp.com']
        self.sender_email = "sender@openlxp.com"

        self.metadata_df = pd.DataFrame.from_dict({1: self.source_metadata},
                                                  orient='index')

        return super().setUp()

    def tearDown(self):
        return super().tearDown()
