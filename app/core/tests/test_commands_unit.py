import logging
from unittest.mock import patch

import pandas as pd
from ddt import ddt
from django.core.management import call_command
from django.db.utils import OperationalError
from django.test import tag
from django.utils import timezone

from core.management.commands.extract_source_metadata import (
    add_publisher_to_source, extract_metadata_using_key, get_publisher_detail,
    get_source_metadata)
from core.management.commands.validate_source_metadata import (
    get_source_metadata_for_validation, validate_source_using_key)
from core.management.commands.load_target_metadata import (
    check_records_to_load_into_xis, get_publisher_to_add, post_data_to_xis,
    renaming_xia_for_posting_to_xis)
from core.management.commands.transform_source_metadata import (
    create_target_metadata_dict, get_source_metadata_for_transformation,
    transform_source_using_key)
from core.management.commands.validate_target_metadata import (
    get_target_metadata_for_validation, validate_target_using_key)
from core.models import MetadataLedger, XIAConfiguration

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
@ddt
class CommandTests(TestSetUp):

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

    def test_get_publisher_detail(self):
        """Test to retrieve publisher from XIA configuration"""
        with patch('core.management.commands.extract_source_metadata'
                   '.XIAConfiguration.objects') as xdsCfg:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xdsCfg.first.return_value = xiaConfig
            return_from_function = get_publisher_detail()
            self.assertEqual(xiaConfig.publisher, return_from_function)

    def test_get_source_metadata(self):
        """ Test to retrieving source metadata"""
        with patch('core.management.commands.extract_source_metadata'
                   '.read_source_file') as read_obj:
            read_obj.return_value = read_obj
            read_obj.return_value = dict({1: {'a': 'b'}})
            return_from_function = get_source_metadata()
            self.assertEqual(read_obj.return_value,
                             return_from_function)

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        test_data = {
            "key1": ["val1"],
            "key2": ["val2"],
            "key3": ["val3"]}

        test_df = pd.DataFrame.from_dict(test_data)
        result = add_publisher_to_source(test_df, 'JKO')
        key_exist = 'SOURCESYSTEM' in result[0]
        self.assertTrue(key_exist)

    def test_extract_metadata_using_key(self):
        """Test to creating key, hash of key & hash of metadata"""
        data = {1: self.source_metadata}
        with patch('core.management.commands.extract_source_metadata'
                   '.get_source_metadata_key_value', return_value=None) as \
                mock_get_source, \
                patch('core.management.commands.extract_source_metadata'
                      '.store_source_metadata',
                      return_value=None) as mock_store_source:
            mock_get_source.return_value = mock_get_source
            mock_get_source.exclude.return_value = mock_get_source
            mock_get_source.filter.side_effect = [
                mock_get_source, mock_get_source]

            extract_metadata_using_key(data)
            self.assertEqual(mock_get_source.call_count, 1)
            self.assertEqual(mock_store_source.call_count, 1)

    # Test cases for validate_source_metadata

    def test_get_source_metadata_for_validation(self):
        """Test to Retrieving source metadata from MetadataLedger that needs
        to be validated"""
        with patch('core.management.commands.validate_source_metadata'
                   '.MetadataLedger.objects') as meta_obj:
            meta_ledger = MetadataLedger.objects.values(
                source_metadata=self.source_metadata).filter(
                source_metadata_validation_status='',
                record_lifecycle_status='Active').exclude(
                source_metadata_extraction_date=None)
            meta_obj.first.return_value = meta_ledger
            return_from_function = get_source_metadata_for_validation()
            self.assertEqual(meta_obj.first.return_value,
                             return_from_function)

    def test_validate_source_using_key_more_than_one(self):
        """Test to Validating source data against required & recommended
        column names for more than one row"""
        data = [{1: self.source_metadata}, {2: self.source_metadata}]

        recommended_column_name = []
        with patch('core.management.commands.validate_source_metadata'
                   '.get_source_metadata_key_value',
                   return_value=None) as mock_get_source_kv, \
                patch('core.management.commands.validate_source_metadata'
                      '.store_source_metadata_validation_status',
                      return_value=None) as mock_store_source_valid_status:
            mock_get_source_kv.return_value = mock_get_source_kv
            mock_get_source_kv.exclude.return_value = mock_get_source_kv
            mock_get_source_kv.filter.side_effect = [
                mock_get_source_kv, mock_get_source_kv]

            validate_source_using_key(data, self.test_required_column_names,
                                      recommended_column_name)
            self.assertEqual(
                mock_store_source_valid_status.call_count, 2)

    def test_validate_source_using_key_more_than_zero(self):
        """Test to Validating source data against required/ recommended column
            namess with no data"""
        data = []
        recommended_column_name = []
        with patch('core.management.commands.validate_source_metadata'
                   '.get_source_metadata_key_value',
                   return_value=None) as mock_get_source_kv, \
                patch('core.management.commands.validate_source_metadata'
                      '.store_source_metadata_validation_status',
                      return_value=None) as mock_store_source_valid_status:
            mock_get_source_kv.return_value = mock_get_source_kv
            mock_get_source_kv.exclude.return_value = mock_get_source_kv
            mock_get_source_kv.filter.side_effect = [
                mock_get_source_kv, mock_get_source_kv]

            validate_source_using_key(data, self.test_required_column_names,
                                      recommended_column_name)
            self.assertEqual(
                mock_store_source_valid_status.call_count, 0)

    # Test cases for transform_source_metadata

    def test_get_source_metadata_for_transformation(self):
        """Test to Retrieving Source metadata from MetadataLedger that needs to be
        transformed"""
        with patch('core.management.commands.transform_source_metadata'
                   '.MetadataLedger.objects') as meta_obj:
            target_data_dict = MetadataLedger.objects.values(
                source_data_dict=self.source_metadata).filter(
                source_metadata_validation_status='Y',
                record_lifecycle_status='Active').exclude(
                source_metadata_validation_date=None)
            meta_obj.first.return_value = target_data_dict
            return_from_function = get_source_metadata_for_transformation()
            self.assertEqual(meta_obj.first.return_value,
                             return_from_function)

    def test_create_target_metadata_dict(self):
        """Test for a function to replace and transform source data to target
        data for using target mapping schema"""
        expected_data_dict = {0: self.target_metadata}
        with patch('core.management.utils.xia_internal.dict_flatten',
                   return_value=self.source_metadata):
            result_data_dict = create_target_metadata_dict(
                self.source_target_mapping, self.source_metadata,
                self.test_required_column_names)
            self.assertEqual(result_data_dict[0]['Course'].get('CourseCode'),
                             expected_data_dict[0]['Course'].get('CourseCode'))
            self.assertEqual(
                result_data_dict[0]['Course'].get('CourseProviderName'),
                expected_data_dict[0]['Course'].get('CourseProviderName'))

    def test_transform_source_using_key_more_zero(self):
        """Test for transforming source data using target metadata schema
        with no data"""
        data = []
        with patch('core.management.utils.xia_internal'
                   '.get_target_metadata_key_value',
                   return_value=None), \
                patch('core.management.commands.transform_source_metadata'
                      '.store_transformed_source_metadata',
                      return_value=None) as mock_store_transformed_source:
            mock_store_transformed_source.return_value = \
                mock_store_transformed_source
            mock_store_transformed_source.exclude.return_value = \
                mock_store_transformed_source
            mock_store_transformed_source.filter.side_effect = [
                mock_store_transformed_source, mock_store_transformed_source]

            transform_source_using_key(data, self.source_target_mapping,
                                       self.test_required_column_names)

            self.assertEqual(
                mock_store_transformed_source.call_count, 0)

    def test_transform_source_using_key_more_than_one(self):
        """Test for transforming source data using target metadata schema for
        more than one row"""
        data = [{0: self.source_metadata},
                {1: self.source_metadata}]
        with patch('core.management.utils.xia_internal'
                   '.get_target_metadata_key_value',
                   return_value=None), \
                patch('core.management.commands.transform_source_metadata'
                      '.store_transformed_source_metadata',
                      return_value=None) as mock_store_transformed_source:
            mock_store_transformed_source.return_value = \
                mock_store_transformed_source
            mock_store_transformed_source.exclude.return_value = \
                mock_store_transformed_source
            mock_store_transformed_source.filter.side_effect = [
                mock_store_transformed_source, mock_store_transformed_source]

            transform_source_using_key(data, self.source_target_mapping,
                                       self.test_required_column_names)

            self.assertEqual(
                mock_store_transformed_source.call_count, 2)

    # Test cases for validate_target_metadata

    def test_get_target_metadata_for_validation(self):
        """Test to Retrieving target metadata from MetadataLedger that needs
        to be validated"""
        with patch('core.management.commands.validate_target_metadata'
                   '.MetadataLedger.objects') as meta_obj:
            target_data_dict = MetadataLedger.objects.values(
                target_metadata=self.target_metadata).filter(
                target_metadata_validation_status='',
                record_lifecycle_status='Active').exclude(
                source_metadata_transformation_date=None)
            meta_obj.first.return_value = target_data_dict
            return_from_function = get_target_metadata_for_validation()
            self.assertEqual(meta_obj.first.return_value,
                             return_from_function)

    def test_validate_target_using_key_more_than_one(self):
        """Test to Validating target data against required & recommended
        column names for more than one row"""
        data = [{1: self.target_metadata}, {2: self.target_metadata}]
        test_required_column_names = {
            'CourseInstance.EndDate', 'CourseInstance.DeliveryMode',
            'CourseInstance.CourseCode', 'CourseInstance.Instructor',
            'Course.CourseProviderName', 'Course.CourseDescription',
            'Course.CourseShortDescription', 'CourseInstance.StartDate',
            'Course.CourseCode', 'CourseInstance.CourseTitle ',
            'General_Information.StartDate', 'Course.CourseSubjectMatter',
            'General_Information.EndDate', 'Course.CourseTitle'}
        recommended_column_name = {'Technical_Information.Thumbnail',
                                   'CourseInstance.Thumbnail'}
        with patch('core.management.commands.validate_target_metadata'
                   '.get_target_metadata_key_value',
                   return_value=None) as mock_get_target_kv, \
                patch('core.management.commands.validate_target_metadata'
                      '.store_target_metadata_validation_status',
                      return_value=None) as mock_store_target_valid_status:
            mock_get_target_kv.return_value = mock_get_target_kv
            mock_get_target_kv.exclude.return_value = mock_get_target_kv
            mock_get_target_kv.filter.side_effect = [
                mock_get_target_kv, mock_get_target_kv]

            validate_target_using_key(data, test_required_column_names,
                                      recommended_column_name)
            self.assertEqual(
                mock_store_target_valid_status.call_count, 2)

    def test_validate_target_using_key_zero(self):
        """Validating target data against required & recommended column names
        with no data"""

        data = []
        test_required_column_names = {
            'CourseInstance.EndDate', 'CourseInstance.DeliveryMode',
            'CourseInstance.CourseCode', 'CourseInstance.Instructor',
            'Course.CourseProviderName', 'Course.CourseDescription',
            'Course.CourseShortDescription', 'CourseInstance.StartDate',
            'Course.CourseCode', 'CourseInstance.CourseTitle ',
            'General_Information.StartDate', 'Course.CourseSubjectMatter',
            'General_Information.EndDate', 'Course.CourseTitle'}
        recommended_column_name = {'Technical_Information.Thumbnail',
                                   'CourseInstance.Thumbnail'}
        with patch('core.management.commands.validate_target_metadata'
                   '.get_target_metadata_key_value',
                   return_value=None) as mock_get_target_kv, \
                patch('core.management.commands.validate_target_metadata'
                      '.store_target_metadata_validation_status',
                      return_value=None) as mock_store_target_valid_status:
            mock_get_target_kv.return_value = mock_get_target_kv
            mock_get_target_kv.exclude.return_value = mock_get_target_kv
            mock_get_target_kv.filter.side_effect = [
                mock_get_target_kv, mock_get_target_kv]

            validate_target_using_key(data, test_required_column_names,
                                      recommended_column_name)

            self.assertEqual(mock_store_target_valid_status.call_count, 0)

    # Test cases for load_target_metadata

    def test_get_publisher_to_add(self):
        """Test to Retrieve publisher from XIA configuration"""
        with patch('core.management.commands.load_target_metadata'
                   '.XIAConfiguration.objects') as xisCfg:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            return_from_function = get_publisher_to_add()
            self.assertEqual(xiaConfig.publisher, return_from_function)

    def test_renaming_xia_for_posting_to_xis(self):
        """Test for Renaming XIA column names to match with XIS column names"""
        with patch('core.management.commands.load_target_metadata'
                   '.get_publisher_to_add') as response_obj:
            response_obj.return_value = response_obj
            response_obj.return_value = 'JKO'
            return_data = renaming_xia_for_posting_to_xis(self.xia_data)
        self.assertEquals(self.xis_expected_data['metadata_hash'],
                          return_data['metadata_hash'])
        self.assertEquals(self.xis_expected_data['metadata_key'],
                          return_data['metadata_key'])
        self.assertEquals(self.xis_expected_data['metadata_key_hash'],
                          return_data['metadata_key_hash'])
        self.assertEquals(self.xis_expected_data['provider_name'],
                          return_data['provider_name'])

    def test_check_records_to_load_into_xis_one_record(self):
        """Test to Retrieve number of Metadata_Ledger records in XIA to load
        into XIS  and calls the post_data_to_xis accordingly"""
        with patch('core.management.commands.load_target_metadata'
                   '.post_data_to_xis', return_value=None)as \
                mock_post_data_to_xis, \
                patch('core.management.commands.load_target_metadata'
                      '.MetadataLedger.objects') as meta_obj:
            meta_data = MetadataLedger(
                record_lifecycle_status='Active',
                source_metadata=self.source_metadata,
                target_metadata=self.target_metadata,
                target_metadata_hash=self.target_hash_value,
                target_metadata_key_hash=self.target_key_value_hash,
                target_metadata_key=self.target_key_value,
                source_metadata_transformation_date=timezone.now(),
                target_metadata_validation_status='Y',
                source_metadata_validation_status='Y',
                target_metadata_transmission_status='Ready')
            meta_obj.return_value = meta_obj
            meta_obj.exclude.return_value = meta_obj
            meta_obj.values.return_value = [meta_data]
            meta_obj.filter.side_effect = [meta_obj, meta_obj]
            check_records_to_load_into_xis()
            self.assertEqual(
                mock_post_data_to_xis.call_count, 1)

    def test_check_records_to_load_into_xis_zero(self):
        """Test to Retrieve number of Metadata_Ledger records in XIA to load
        into XIS  and calls the post_data_to_xis accordingly"""
        with patch('core.management.commands.load_target_metadata'
                   '.post_data_to_xis', return_value=None)as \
                mock_post_data_to_xis, \
                patch('core.management.commands.load_target_metadata'
                      '.MetadataLedger.objects') as meta_obj:
            meta_obj.return_value = meta_obj
            meta_obj.exclude.return_value = meta_obj
            meta_obj.filter.side_effect = [meta_obj, meta_obj]
            check_records_to_load_into_xis()
            self.assertEqual(
                mock_post_data_to_xis.call_count, 0)

    def test_post_data_to_xis_zero(self):
        """Test for POSTing XIA metadata_ledger to XIS metadata_ledger
        when data is not present"""
        data = []
        with patch('core.management.commands.load_target_metadata'
                   '.renaming_xia_for_posting_to_xis',
                   return_value=self.xis_expected_data), \
                patch('core.management.commands.load_target_metadata'
                      '.XIAConfiguration.objects') as xisCfg, \
                patch('core.management.commands.load_target_metadata'
                      '.MetadataLedger.objects') as meta_obj, \
                patch('requests.post') as response_obj, \
                patch('core.management.commands.load_target_metadata'
                      '.check_records_to_load_into_xis',
                      return_value=None) as mock_check_records_to_load:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            response_obj.return_value = response_obj
            response_obj.status_code = 201

            meta_obj.return_value = meta_obj
            meta_obj.exclude.return_value = meta_obj
            meta_obj.update.return_value = meta_obj
            meta_obj.filter.side_effect = [meta_obj, meta_obj, meta_obj,
                                           meta_obj]

            post_data_to_xis(data)
            self.assertEqual(response_obj.call_count, 0)
            self.assertEqual(mock_check_records_to_load.call_count, 1)

    def test_post_data_to_xis_more_than_one(self):
        """Test for POSTing XIA metadata_ledger to XIS metadata_ledger
        when more than one rows are passing"""
        data = [self.xia_data,
                self.xia_data]
        with patch('core.management.commands.load_target_metadata'
                   '.renaming_xia_for_posting_to_xis',
                   return_value=self.xis_expected_data), \
                patch('core.management.commands.load_target_metadata'
                      '.XIAConfiguration.objects') as xisCfg, \
                patch('core.management.commands.load_target_metadata'
                      '.MetadataLedger.objects') as meta_obj, \
                patch('requests.post') as response_obj, \
                patch('core.management.commands.load_target_metadata'
                      '.check_records_to_load_into_xis',
                      return_value=None) as mock_check_records_to_load:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            response_obj.return_value = response_obj
            response_obj.status_code = 201

            meta_obj.return_value = meta_obj
            meta_obj.exclude.return_value = meta_obj
            meta_obj.update.return_value = meta_obj
            meta_obj.filter.side_effect = [meta_obj, meta_obj, meta_obj,
                                           meta_obj]

            post_data_to_xis(data)
            self.assertEqual(response_obj.call_count, 2)
            self.assertEqual(mock_check_records_to_load.call_count, 1)
