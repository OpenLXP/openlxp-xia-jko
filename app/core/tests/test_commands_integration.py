import logging
from unittest.mock import patch
from uuid import UUID

from django.test import TestCase, tag
from django.utils import timezone

from core.management.commands.extract_source_metadata import (
    extract_metadata_using_key, get_publisher_detail, store_source_metadata)
from core.management.commands.load_target_metadata import (
    post_data_to_xis, renaming_xia_for_posting_to_xis)
from core.management.commands.transform_source_metadata import (
    get_target_metadata_for_transformation, transform_source_using_key)
from core.management.commands.validate_source_metadata import (
    get_source_metadata_for_validation, get_source_validation_schema,
    validate_source_using_key)
from core.management.commands.validate_target_metadata import (
    get_target_validation_schema, validate_target_using_key)
from core.management.utils.xss_client import read_json_data
from core.models import MetadataLedger, XIAConfiguration

logger = logging.getLogger('dict_config_logger')


@tag('integration')
class Command(TestCase):
    # globally accessible data sets
    metadata = {
        "Duration": 1,
        "Instance": 12345,
        "CatalogURL": "https://test_data.com",
        "DeliveryMode": "test_data",
        "SOURCESYSTEM": "JKO",
        "LearningResourceName": "test_data",
        "LearningResourceIdentifier": "test_data",
        "LearningResourceDescription": "test_data"
    }

    key_value = "test_data_JKO"
    key_value_hash = "2e08c9810d5cb3127761ec4a974b2d5d"
    hash_value = "cb9af09d61c63b11832d9f30ae215829"

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

    target_key_value = "test_data_JKO"
    target_key_value_hash = "2e08c9810d5cb3127761ec4a974b2d5d"
    target_hash_value = "de7f0a54a5895db52029ab2c2405aea5"

    metadata_invalid = {
        "Duration": 1,
        "Instance": 12345,
        "CatalogURL": "https://test_data.com",
        "DeliveryMode": "test_data",
        "SOURCESYSTEM": "JKO",
        "LearningResourceName": "test_data1",
        "LearningResourceIdentifier": "test_data1",
        "LearningResourceDescription": ""
    }

    key_value_invalid = "test_data1_JKO"
    key_value_hash_invalid = "010cc8e51aef01eb7a7cd2888eda3d06"
    hash_value_invalid = "f15860eed6e88ddfcce71059043f34a3"

    target_metadata_invalid = {
        "Course": {
            "CourseCode": "test_data1",
            "CourseType": "test_data",
            "CourseTitle": "test_data",
            "CourseDescription": "",
            "CourseProviderName": "JKO",
            "EstimatedCompletionTime": 1
        },
        "CourseInstance": {
            "CourseURL": "https://test_data.com"
        }
    }
    target_key_value_invalid = "test_data1_JKO"
    target_key_value_hash_invalid = "010cc8e51aef01eb7a7cd2888eda3d06"
    target_hash_value_invalid = "f15860eed6e88ddfcce71059043f34a3"

    target_mapping_dict = {
        "Course": {
            "CourseProviderName": "SOURCESYSTEM",
            "CourseCode": "LearningResourceIdentifier",
            "CourseType": "DeliveryMode",
            "CourseTitle": "LearningResourceName",
            "CourseDescription": "LearningResourceDescription",
            "EstimatedCompletionTime": "Duration"
        },
        "CourseInstance": {
            "CourseURL": "CatalogURL"
        }
    }

    xia_data = {
        'metadata_record_uuid': UUID('6ee3a160-ef1f-4246-97ec-7f324b2155a5'),
        'target_metadata': {
            'Course': {
                'CourseCode': 'test_data',
                'CourseType': 'test_data',
                'CourseTitle': 'test_data',
                'CourseDescription': 'test_data',
                'CourseProviderName': 'JKO',
                'EstimatedCompletionTime': 1
            },
            'CourseInstance': {
                'CourseURL': 'https://test_data.com'
            }
        },
        'target_metadata_hash': 'de7f0a54a5895db52029ab2c2405aea5',
        'target_metadata_key': 'test_data_JKO',
        'target_metadata_key_hash': '2e08c9810d5cb3127761ec4a974b2d5d'
    }

    xis_expected_data = {
        'unique_record_identifier': '6ee3a160-ef1f-4246-97ec-7f324b2155a5',
        'metadata': {
            'Course': {
                'CourseCode': 'test_data',
                'CourseType': 'test_data',
                'CourseTitle': 'test_data',
                'CourseDescription ': 'test_data',
                'CourseProviderName ': 'JKO',
                'EstimatedCompletionTime ': 1
            },
            'CourseInstance': {
                'CourseURL': 'https://test_data.com'
            }
        },
        'metadata_hash': 'de7f0a54a5895db52029ab2c2405aea5',
        'metadata_key': 'test_data_JKO',
        'metadata_key_hash': '2e08c9810d5cb3127761ec4a974b2d5d',
        'provider_name': 'JKO'
    }

    # Test cases for extract_source_metadata

    def test_get_publisher_detail(self):
        """Test to get publisher name from XIAConfiguration """

        xiaConfig = XIAConfiguration(publisher='JKO')
        xiaConfig.save()
        result_publisher = get_publisher_detail()
        self.assertEqual('JKO', result_publisher)

    def test_store_source_metadata(self):
        """Test to extract data from Experience Source Repository(XSR)
        and store in metadata ledger """
        store_source_metadata(self.key_value, self.key_value_hash,
                              self.hash_value,
                              self.metadata)

        result_query = MetadataLedger.objects.values(
            'source_metadata_key',
            'source_metadata_key_hash',
            'source_metadata',
            'source_metadata_hash',
            'record_lifecycle_status').filter(
            source_metadata_key_hash=self.key_value_hash).first()

        self.assertEqual(self.key_value, result_query.get(
            'source_metadata_key'))
        self.assertEqual(self.hash_value, result_query.get(
            'source_metadata_hash'))
        self.assertEqual(self.metadata, result_query.get(
            'source_metadata'))
        self.assertEqual('Active', result_query.get(
            'record_lifecycle_status'))

    def test_extract_metadata_using_key(self):
        """Test for the keys and hash creation and save in
        Metadata_ledger table """
        data = {1: self.metadata}
        extract_metadata_using_key(data)
        result_query = MetadataLedger.objects.values(
            'source_metadata_key',
            'source_metadata_key_hash',
            'source_metadata',
            'source_metadata_hash',
        ).filter(
            source_metadata_key=self.key_value).first()

        self.assertEqual(self.key_value, result_query.get(
            'source_metadata_key'))
        self.assertEqual(self.hash_value, result_query.get(
            'source_metadata_hash'))
        self.assertEqual(self.key_value_hash, result_query.get(
            'source_metadata_key_hash'))
        self.assertEqual(self.metadata, result_query.get(
            'source_metadata'))

    # Test cases for validate_source_metadata

    def test_get_source_validation_schema(self):
        """Test to retrieve source validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(
            source_metadata_schema='JKO_source_validate_schema.json')
        xiaConfig.save()
        result_dict = get_source_validation_schema()
        expected_dict = read_json_data('JKO_source_validate_schema.json')
        self.assertEqual(expected_dict, result_dict)

    def test_get_source_metadata_for_validation(self):
        """Test retrieving  source metadata from MetadataLedger that
        needs to be validated"""

        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            source_metadata_hash=self.hash_value,
            source_metadata_key=self.key_value,
            source_metadata_key_hash=self.key_value_hash,
            source_metadata_extraction_date=timezone.now())
        metadata_ledger.save()
        test_source_data = get_source_metadata_for_validation()
        self.assertTrue(test_source_data)

    def test_validate_source_using_key(self):
        """Test to check validation process for source"""

        test_required_column_names = ['SOURCESYSTEM',
                                      'LearningResourceIdentifier',
                                      'LearningResourceName',
                                      'LearningResourceDescription']
        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            source_metadata_hash=self.hash_value,
            source_metadata_key=self.key_value,
            source_metadata_key_hash=self.key_value_hash,
            source_metadata_extraction_date=timezone.now())
        metadata_ledger_invalid = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata_invalid,
            source_metadata_hash=self.hash_value_invalid,
            source_metadata_key=self.key_value_invalid,
            source_metadata_key_hash=self.key_value_hash_invalid,
            source_metadata_extraction_date=timezone.now())
        metadata_ledger.save()
        metadata_ledger_invalid.save()
        result_test_query = MetadataLedger.objects. \
            values('source_metadata')
        validate_source_using_key(result_test_query,
                                  test_required_column_names)
        result_query = MetadataLedger.objects. \
            values('source_metadata_validation_status',
                   'record_lifecycle_status'). \
            filter(source_metadata_key=self.key_value).first()
        result_query_invalid = MetadataLedger.objects. \
            values('source_metadata_validation_status',
                   'record_lifecycle_status'). \
            filter(source_metadata_key=self.key_value_invalid).first()
        self.assertEqual('Y',
                         result_query['source_metadata_validation_status'])
        self.assertEqual('Active',
                         result_query['record_lifecycle_status'])
        self.assertEqual('N', result_query_invalid[
            'source_metadata_validation_status'])
        self.assertEqual('Inactive',
                         result_query_invalid['record_lifecycle_status'])

    # Test cases for transform_source_metadata

    def test_get_target_metadata_for_transformation(self):
        """Test that get target mapping_dictionary from XIAConfiguration """

        xiaConfig = XIAConfiguration(
            source_target_mapping='JKO_p2881_target_metadata_schema.json')
        xiaConfig.save()
        result_target_mapping_dict = get_target_metadata_for_transformation()
        expected_target_mapping_dict = read_json_data(
            'JKO_p2881_target_metadata_schema.json')
        self.assertEqual(result_target_mapping_dict,
                         expected_target_mapping_dict)

    def test_transform_source_using_key(self):
        """Test to transform source metadata to target metadata schema
        format"""
        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            source_metadata_hash=self.key_value_hash,
            source_metadata_validation_status='Y',
            source_metadata_key=self.key_value,
            source_metadata_validation_date=timezone.now())
        metadata_ledger.save()

        test_data_dict = MetadataLedger.objects.values(
            'source_metadata').filter(
            source_metadata_validation_status='Y',
            record_lifecycle_status='Active').exclude(
            source_metadata_validation_date=None)

        transform_source_using_key(test_data_dict, self.target_mapping_dict)
        result_data = MetadataLedger.objects.filter(
            source_metadata_key=self.key_value,
            record_lifecycle_status='Active',
            source_metadata_validation_status='Y'
        ).values(
            'source_metadata_transformation_date',
            'target_metadata_key',
            'target_metadata_key_hash',
            'target_metadata',
            'target_metadata_hash').first()

        self.assertTrue(result_data.get('source_metadata_transformation_date'))
        self.assertTrue(result_data.get('target_metadata_key'))
        self.assertTrue(result_data.get('target_metadata_key_hash'))
        self.assertTrue(result_data.get('target_metadata'))
        self.assertTrue(result_data.get('target_metadata_hash'))

    # Test cases for validate_target_metadata

    def test_get_target_validation_schema(self):
        """Test to Retrieve target validation schema from XIA configuration """
        xiaConfig = XIAConfiguration(
            target_metadata_schema='p2881_target_validation_schema.json')
        xiaConfig.save()
        result_target_metadata_schema = get_target_validation_schema()
        self.assertEqual('p2881_target_validation_schema.json',
                         result_target_metadata_schema)

    def test_validate_target_using_key(self):
        """Test for Validating target data for required columns """
        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            target_metadata=self.target_metadata,
            target_metadata_hash=self.target_hash_value,
            target_metadata_key_hash=self.target_key_value_hash,
            target_metadata_key=self.target_key_value,
            source_metadata_transformation_date=timezone.now())
        metadata_ledger.save()
        metadata_ledger_invalid = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata_invalid,
            target_metadata=self.target_metadata_invalid,
            target_metadata_hash=self.target_hash_value_invalid,
            target_metadata_key_hash=self.target_key_value_hash_invalid,
            target_metadata_key=self.target_key_value_invalid,
            source_metadata_transformation_date=timezone.now())
        metadata_ledger_invalid.save()
        test_data = MetadataLedger.objects.values(
            'target_metadata').filter(target_metadata_validation_status='',
                                      record_lifecycle_status='Active'
                                      ).exclude(
            source_metadata_transformation_date=None)
        required_dict = {'Course': ['CourseProviderName', 'CourseCode',
                                    'CourseTitle', 'CourseDescription',
                                    'CourseShortDescription',
                                    'CourseSubjectMatter'],
                         'CourseInstance': ['CourseCode', 'CourseTitle ',
                                            'StartDate', 'EndDate',
                                            'DeliveryMode', 'Instructor'],
                         'General_Information': ['StartDate', 'EndDate'],
                         'Technical_Information': []}
        recommended_dict = {'Course': [], 'CourseInstance': ['Thumbnail'],
                            'General_Information': [],
                            'Technical_Information': ['Thumbnail']}
        validate_target_using_key(test_data, required_dict, recommended_dict)
        result_query = MetadataLedger.objects.values(
            'target_metadata_validation_status', 'record_lifecycle_status'). \
            filter(target_metadata_key_hash=self.target_key_value_hash).first()
        result_query_invalid = MetadataLedger.objects.values(
            'target_metadata_validation_status', 'record_lifecycle_status'). \
            filter(target_metadata_key_hash=self.
                   target_key_value_hash_invalid).first()
        self.assertEqual('Y', result_query.get(
            'target_metadata_validation_status'))
        self.assertEqual('Active', result_query.get(
            'record_lifecycle_status'))
        self.assertEqual('N', result_query_invalid.get(
            'target_metadata_validation_status'))
        self.assertEqual('Inactive', result_query_invalid.get(
            'record_lifecycle_status'))

    # Test cases for load_target_metadata

    def test_renaming_xia_for_posting_to_xis(self):
        """Test for Renaming XIA column names to match with XIS column names"""
        xiaConfig = XIAConfiguration(publisher='JKO')
        xiaConfig.save()

        return_data = renaming_xia_for_posting_to_xis(self.xia_data)
        self.assertEquals(self.xis_expected_data['metadata_hash'],
                          return_data['metadata_hash'])
        self.assertEquals(self.xis_expected_data['metadata_key'],
                          return_data['metadata_key'])
        self.assertEquals(self.xis_expected_data['metadata_key_hash'],
                          return_data['metadata_key_hash'])
        self.assertEquals(self.xis_expected_data['provider_name'],
                          return_data['provider_name'])

    def test_post_data_to_xis_response_201(self):
        """POSTing XIA metadata_ledger to XIS metadata_ledger and receive
        response status code 201"""

        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            target_metadata=self.target_metadata,
            target_metadata_hash=self.target_hash_value,
            target_metadata_key_hash=self.target_key_value_hash,
            target_metadata_key=self.target_key_value,
            source_metadata_transformation_date=timezone.now(),
            target_metadata_validation_status='Y',
            source_metadata_validation_status='Y',
            target_metadata_transmission_status='Ready')
        metadata_ledger.save()
        data = MetadataLedger.objects.filter(
            record_lifecycle_status='Active',
            target_metadata_validation_status='Y',
            target_metadata_transmission_status='Ready').values(
            'metadata_record_uuid',
            'target_metadata',
            'target_metadata_hash',
            'target_metadata_key',
            'target_metadata_key_hash')
        xiaConfig = XIAConfiguration(publisher='DAU')
        xiaConfig.save()
        with patch('requests.post') as response_obj:
            response_obj.return_value = response_obj
            response_obj.status_code = 201

            post_data_to_xis(data)
            result_query = MetadataLedger.objects.values(
                'target_metadata_transmission_status_code',
                'target_metadata_transmission_status').filter(
                target_metadata_key=self.target_key_value).first()

            self.assertEqual(201, result_query.get(
                'target_metadata_transmission_status_code'))
            self.assertEqual('Successful', result_query.get(
                'target_metadata_transmission_status'))

    def test_post_data_to_xis_responses_other_than_201(self):
        """POSTing XIA metadata_ledger to XIS metadata_ledger and receive
        response status code 201"""
        metadata_ledger = MetadataLedger(
            record_lifecycle_status='Active',
            source_metadata=self.metadata,
            target_metadata=self.target_metadata,
            target_metadata_hash=self.target_hash_value,
            target_metadata_key_hash=self.target_key_value_hash,
            target_metadata_key=self.target_key_value,
            source_metadata_transformation_date=timezone.now(),
            target_metadata_validation_status='Y',
            source_metadata_validation_status='Y',
            target_metadata_transmission_status='Ready')
        metadata_ledger.save()
        data = MetadataLedger.objects.filter(
            record_lifecycle_status='Active',
            target_metadata_validation_status='Y',
            target_metadata_transmission_status='Ready').values(
            'metadata_record_uuid',
            'target_metadata',
            'target_metadata_hash',
            'target_metadata_key',
            'target_metadata_key_hash')
        xiaConfig = XIAConfiguration(publisher='DAU')
        xiaConfig.save()
        with patch('requests.post') as response_obj:
            response_obj.return_value = response_obj
            response_obj.status_code = 400
            post_data_to_xis(data)
            result_query = MetadataLedger.objects.values(
                'target_metadata_transmission_status_code',
                'target_metadata_transmission_status').filter(
                target_metadata_key=self.target_key_value).first()
            self.assertEqual(400, result_query.get(
                'target_metadata_transmission_status_code'))
            self.assertEqual('Failed', result_query.get(
                'target_metadata_transmission_status'))
