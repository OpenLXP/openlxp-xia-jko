import hashlib
import json
import logging

import pandas as pd
from django.core.management.base import BaseCommand
from django.utils import timezone

from core.management.utils.xia_internal import (convert_date_to_isoformat,
                                                get_publisher_detail,
                                                get_source_metadata_key_value)
from core.management.utils.xsr_client import read_source_file
from core.models import MetadataLedger, MetadataFieldOverwrite

logger = logging.getLogger('dict_config_logger')


def get_source_metadata():
    """Retrieving source metadata"""

    #  Retrieve metadata from agents as a list of sources
    df_source_list = read_source_file()

    # Iterate through the list of sources and extract metadata
    for source_item in df_source_list:
        logger.info('Loading metadata to be extracted from source')

        # Changing null values to None for source dataframe
        std_source_df = source_item.where(pd.notnull(source_item),
                                          None)
        if std_source_df.empty:
            logger.error("Source metadata is empty!")
        extract_metadata_using_key(std_source_df)


def add_publisher_to_source(source_df):
    """Add publisher column to source metadata and return source metadata"""

    # Get publisher name from system operator
    publisher = get_publisher_detail()
    if not publisher:
        logger.warning("Publisher field is empty!")
    # Assign publisher column to source data
    source_df['SOURCESYSTEM'] = publisher
    return source_df


def override_metadata_field(source_df):
    """Overwrite & append metadata fields with admin entered values """
    logger.info("Overwrite & append metadata fields with admin entered values")

    # looping through fields to be override or appended
    for each in MetadataFieldOverwrite.objects.all():
        column = each.field_name
        value = each.field_value
        overwrite_flag = each.overwrite

        # field should be overwritten and append
        if overwrite_flag:
            source_df[column] = value
        # skip field to be overwritten and append
        else:
            if column not in source_df.columns:
                source_df[column] = value
            else:
                source_df.loc[source_df[column].isnull(), column] = value
    #  return source metadata as dictionary
    source_data_dict = source_df.to_dict(orient='index')
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
    MetadataLedger.objects.get_or_create(
        source_metadata_key=key_value,
        source_metadata_key_hash=key_value_hash,
        source_metadata=metadata,
        source_metadata_hash=hash_value,
        record_lifecycle_status='Active')


def extract_metadata_using_key(source_df):
    """Creating key, hash of key & hash of metadata """
    # Convert source data to dictionary and add publisher to metadata
    source_df = add_publisher_to_source(source_df)
    source_data_dict = override_metadata_field(source_df)

    logger.info('Setting record_status & deleted_date for updated record')
    logger.info('Getting existing records or creating new record to '
                'MetadataLedger')
    for temp_key, temp_val in source_data_dict.items():
        # creating hash value of metadata
        hash_value = hashlib.md5(str(temp_val).encode('utf-8')).hexdigest()
        # key dictionary creation function called
        key = \
            get_source_metadata_key_value(source_data_dict[temp_key])

        # Call store function with key, hash of key, hash of metadata,
        # metadata

        temp_val_convert = json.dumps(temp_val,
                                      default=convert_date_to_isoformat)
        temp_val_json = json.loads(temp_val_convert)
        if key:
            store_source_metadata(key['key_value'], key['key_value_hash'],
                                  hash_value, temp_val_json)


class Command(BaseCommand):
    """Django command to extract data from Experience Source Repository (
    XSR) """

    def handle(self, *args, **options):
        """
            Metadata is extracted from XSR and stored in Metadata Ledger
        """
        get_source_metadata()

        logger.info('MetadataLedger updated with extracted data from XSR')
