import logging
import pandas as pd
from ddt import ddt
from django.test import tag
from core.management.commands.extract_source_metadata import (
    extract_metadata_using_key, store_source_metadata)
from django_openlxp_xia.models import (MetadataLedger, XIAConfiguration)

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('integration')
@ddt
class CommandIntegration(TestSetUp):
    # globally accessible data sets

    # Test cases for extract_source_metadata

    def test_store_source_metadata(self):
        """Test to extract data from Experience Source Repository(XSR)
        and store in metadata ledger """
        store_source_metadata(self.key_value, self.key_value_hash,
                              self.hash_value,
                              self.source_metadata)

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
        self.assertEqual(self.source_metadata, result_query.get(
            'source_metadata'))
        self.assertEqual('Active', result_query.get(
            'record_lifecycle_status'))

    def test_extract_metadata_using_key(self):
        """Test for the keys and hash creation and save in
        Metadata_ledger table """
        input_data = pd.DataFrame.from_dict([self.source_metadata])
        xiaConfig = XIAConfiguration(publisher='JKO')
        xiaConfig.save()
        extract_metadata_using_key(input_data)
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
        self.assertEqual(self.source_metadata, result_query.get(
            'source_metadata'))
