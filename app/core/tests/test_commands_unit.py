import logging
from unittest.mock import patch

import pandas as pd
from ddt import ddt
from django.core.management import call_command
from django.db.utils import OperationalError
from django.test import tag
from openlxp_xia.models import MetadataFieldOverwrite, XIAConfiguration

from core.management.commands.extract_source_metadata import (
    add_publisher_to_source, extract_metadata_using_key,
    get_metadata_fields_to_overwrite, get_source_metadata,
    overwrite_append_metadata, overwrite_metadata_field)

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
    def test_get_source_metadata(self):
        """ Test to retrieving source metadata"""
        with patch('core.management.commands.extract_source_metadata'
                   '.read_source_file') as read_obj, patch(
            'core.management.commands.extract_source_metadata'
            '.extract_metadata_using_key', return_value=None) as \
                mock_extract_obj:
            read_obj.return_value = read_obj
            read_obj.return_value = [
                pd.DataFrame.from_dict(self.test_data, orient='index')]
            get_source_metadata()
            self.assertEqual(mock_extract_obj.call_count, 1)

    def test_add_publisher_to_source(self):
        """Test for Add publisher column to source metadata and return
        source metadata"""
        with patch('openlxp_xia.management.utils.xia_internal'
                   '.get_publisher_detail'), \
                patch('openlxp_xia.management.utils.xia_internal'
                      '.XIAConfiguration.objects') as xisCfg:
            xiaConfig = XIAConfiguration(publisher='JKO')
            xisCfg.first.return_value = xiaConfig
            test_df = pd.DataFrame.from_dict(self.test_data)
            result = add_publisher_to_source(test_df)
            key_exist = 'SOURCESYSTEM' in result.columns
            self.assertTrue(key_exist)

    def test_extract_metadata_using_key(self):
        """Test to creating key, hash of key & hash of metadata"""

        data = {1: self.source_metadata}
        data_df = pd.DataFrame.from_dict(data)
        with patch(
                'core.management.commands.extract_source_metadata'
                '.add_publisher_to_source',
                return_value=data_df), \
                patch('core.management.commands.extract_source_metadata'
                      '.overwrite_metadata_field', return_value=data), \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.get_source_metadata_key_value',
                    return_value=None) as mock_get_source, \
                patch(
                    'core.management.commands.extract_source_metadata'
                    '.store_source_metadata',
                    return_value=None) as mock_store_source:
            mock_get_source.return_value = mock_get_source
            mock_get_source.exclude.return_value = mock_get_source
            mock_get_source.filter.side_effect = [
                mock_get_source, mock_get_source]

            extract_metadata_using_key(data)
            self.assertEqual(mock_get_source.call_count, 1)
            self.assertEqual(mock_store_source.call_count, 1)

    def test_overwrite_metadata_field(self):
        """Test to overwrite metadata with admin entered values and
        return metadata in dictionary format """

        with patch('core.management.commands.extract_source_metadata'
                   '.get_metadata_fields_to_overwrite') as mock_get_overwrite:
            mock_get_overwrite.return_value = self.metadata_df

            return_val = overwrite_metadata_field(self.metadata_df)
            self.assertIsInstance(return_val, dict)

    def test_get_metadata_fields_to_overwrite(self):
        """Test for looping through fields to be overwrite or appended"""
        with patch('core.management.commands.'
                   'extract_source_metadata.MetadataFieldOverwrite.objects') \
                as mock_field, \
                patch('core.management.commands.extract_source_metadata'
                      '.overwrite_append_metadata') as mock_overwrite_fun:
            config = \
                [MetadataFieldOverwrite(field_name='column1', overwrite=True,
                                        field_value='value1'),
                 MetadataFieldOverwrite(field_name='column2', overwrite=False,
                                        field_value='value2')]
            mock_field.all.return_value = config
            mock_overwrite_fun.return_value = self.metadata_df

            return_val = get_metadata_fields_to_overwrite(self.metadata_df)
            print(return_val)
            self.assertEqual(mock_overwrite_fun.call_count, 2)

    def test_overwrite_append_metadata(self):
        """test Overwrite & append metadata fields based on overwrite flag """
        return_val = \
            overwrite_append_metadata(self.metadata_df, 'column1', 'value1',
                                      True)

        self.assertEqual(return_val['column1'].all(), 'value1')
