import hashlib
import logging
from django.core.management.base import BaseCommand
from core.models import XIAConfiguration
from core.models import MetadataLedger
from core.management.utils.xsr_client import read_source_file
from core.management.utils.xia_internal import get_source_metadata_key_value
from django.utils import timezone

logger = logging.getLogger('dict_config_logger')


def get_publisher_detail():
    """Retrieve publisher from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    publisher = xia_data.publisher
    return publisher


def get_source_metadata():
    """Retrieving source metadata"""
    source_df = read_source_file()
    return source_df


def add_publisher_to_source(source_df, publisher):
    """Add publisher column to source metadata and return
        source metadata
    """
    source_df['SOURCESYSTEM'] = publisher
    source_data_dict = source_df.to_dict(orient='index')

    # returning source metadata as dictionary
    return source_data_dict


def store_source_metadata(key_value, key_value_hash, hash_value, metadata):
    """Extract data from Experience Source Repository(XSR)
        and store in metadata ledger
    """
    # Setting record_status & deleted_date for updated record
    MetadataLedger.objects.filter(
        source_metadata_key_hash=key_value_hash,
        record_lifecycle_status='Active').exclude(
        source_metadata_hash=hash_value).update(
        metadata_record_inactivation_date=timezone.now())
    MetadataLedger.objects.filter(
        source_metadata_key_hash=key_value_hash,
        record_lifecycle_status='Active').exclude(
        source_metadata_hash=hash_value).update(
        record_lifecycle_status='Inactive')

    # Retrieving existing records or creating new record to MetadataLedger
    MetadataLedger.objects.get_or_create(source_metadata_key=key_value,
                                         source_metadata_key_hash=
                                         key_value_hash,
                                         source_metadata=metadata,
                                         source_metadata_hash=
                                         hash_value,
                                         record_lifecycle_status=
                                         'Active')


def extract_metadata_using_key(source_data_dict):
    """Creating key, hash of key & hash of metadata """
    logger.info('Setting record_status & deleted_date for updated record')
    logger.info('Getting existing records or creating new record to '
                'MetadataLedger')
    for temp_key, temp_val in source_data_dict.items():
        # creating hash value of metadata
        hash_value = hashlib.md5(str(temp_val).encode('utf-8')).hexdigest()
        for k1, v1 in source_data_dict[temp_key].items():
            # Key value creation for source metadata
            key = \
                get_source_metadata_key_value(k1, source_data_dict[temp_key])
            # Call store function with key, hash of key, hash of metadata,
            # metadata

        store_source_metadata(key['key_value'], key['key_value_hash'],
                              hash_value, temp_val)


class Command(BaseCommand):
    """Django command to extract data from Experience Source Repository (
    XSR) """

    def handle(self, *args, **options):
        """
            Metadata is extracted from XSR and stored in Metadata Ledger
        """
        publisher = get_publisher_detail()
        source_df = get_source_metadata()
        source_data_dict = add_publisher_to_source(source_df, publisher)
        extract_metadata_using_key(source_data_dict)

        logger.info('MetadataLedger updated with extracted data from XSR')
