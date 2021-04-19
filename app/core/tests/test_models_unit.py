from django.test import SimpleTestCase, tag
from django.utils import timezone

from core.models import MetadataLedger, XIAConfiguration


@tag('unit')
class ModelTests(SimpleTestCase):

    def test_create_xia_configuration(self):
        """Test that creating a new XIA Configuration entry is successful
        with defaults """
        source_metadata_schema = 'test_file.json'
        source_target_mapping = 'test_file.json'
        target_metadata_schema = 'test_file.json'

        xiaConfig = XIAConfiguration(
            source_metadata_schema=source_metadata_schema,
            source_target_mapping=source_target_mapping,
            target_metadata_schema=target_metadata_schema)

        self.assertEqual(xiaConfig.source_metadata_schema,
                         source_metadata_schema)
        self.assertEqual(xiaConfig.source_target_mapping,
                         source_target_mapping)
        self.assertEqual(xiaConfig.target_metadata_schema,
                         target_metadata_schema)

    def test_metadata_ledger(self):
        """Test for a new Metadata_Ledger entry is successful with defaults"""
        metadata_record_inactivate_date = timezone.now()
        record_lifecycle_status = 'Active'
        source_metadata = ''
        source_metadata_extraction_date = ''
        source_metadata_hash = '74df499f177d0a7adb3e610302abc6a5'
        source_metadata_key = 'AGENT_test_key'
        source_metadata_key_hash = 'f6df40fbbf4a4c4091fbf64c9b6458e0'
        source_metadata_transform_date = timezone.now()
        source_metadata_validation_date = timezone.now()
        source_metadata_valid_status = 'Y'
        target_metadata = ''
        target_metadata_hash = '74df499f177d0a7adb3e610302abc6a5'
        target_metadata_key = 'AGENT_test_key'
        target_metadata_key_hash = '74df499f177d0a7adb3e610302abc6a5'
        target_metadata_transmit_date = timezone.now()
        target_meta_transmit_status = 'Ready'
        target_transmit_st_code = 200
        target_metadata_validation_date = timezone.now()
        target_metadata_validation_status = 'Y'

        metadataLedger = MetadataLedger(
            metadata_record_inactivation_date=metadata_record_inactivate_date,
            record_lifecycle_status=record_lifecycle_status,
            source_metadata=source_metadata,
            source_metadata_extraction_date=source_metadata_extraction_date,
            source_metadata_hash=source_metadata_hash,
            source_metadata_key=source_metadata_key,
            source_metadata_key_hash=source_metadata_key_hash,
            source_metadata_transformation_date=source_metadata_transform_date,
            source_metadata_validation_date=source_metadata_validation_date,
            source_metadata_validation_status=source_metadata_valid_status,
            target_metadata=target_metadata,
            target_metadata_hash=target_metadata_hash,
            target_metadata_key=target_metadata_key,
            target_metadata_key_hash=target_metadata_key_hash,
            target_metadata_transmission_date=target_metadata_transmit_date,
            target_metadata_transmission_status=target_meta_transmit_status,
            target_metadata_transmission_status_code=target_transmit_st_code,
            target_metadata_validation_date=target_metadata_validation_date,
            target_metadata_validation_status=target_metadata_validation_status
        )

        self.assertEqual(metadataLedger.metadata_record_inactivation_date,
                         metadata_record_inactivate_date)
        self.assertEqual(metadataLedger.record_lifecycle_status,
                         record_lifecycle_status)
        self.assertEqual(metadataLedger.source_metadata, source_metadata)
        self.assertEqual(metadataLedger.source_metadata_extraction_date,
                         source_metadata_extraction_date)
        self.assertEqual(metadataLedger.source_metadata_hash,
                         source_metadata_hash)
        self.assertEqual(metadataLedger.source_metadata_key,
                         source_metadata_key)
        self.assertEqual(metadataLedger.source_metadata_key_hash,
                         source_metadata_key_hash)
        self.assertEqual(metadataLedger.source_metadata_transformation_date,
                         source_metadata_transform_date)
        self.assertEqual(metadataLedger.source_metadata_validation_date,
                         source_metadata_validation_date)
        self.assertEqual(metadataLedger.source_metadata_validation_status,
                         source_metadata_valid_status)
        self.assertEqual(metadataLedger.target_metadata, target_metadata)
        self.assertEqual(metadataLedger.target_metadata_hash,
                         target_metadata_hash)
        self.assertEqual(metadataLedger.target_metadata_key,
                         target_metadata_key)
        self.assertEqual(metadataLedger.target_metadata_key_hash,
                         target_metadata_key_hash)
        self.assertEqual(metadataLedger.target_metadata_transmission_date,
                         target_metadata_transmit_date)
        self.assertEqual(metadataLedger.target_metadata_transmission_status,
                         target_meta_transmit_status)
        self.assertEqual(
            metadataLedger.target_metadata_transmission_status_code,
            target_transmit_st_code)
        self.assertEqual(metadataLedger.target_metadata_validation_date,
                         target_metadata_validation_date)
        self.assertEqual(metadataLedger.target_metadata_validation_status,
                         target_metadata_validation_status)
