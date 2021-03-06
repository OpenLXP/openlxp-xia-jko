import hashlib
import logging

import pandas as pd
from django.core.management.base import BaseCommand
from django.utils import timezone

from core.management.utils.xia_internal import (dict_flatten,
                                                get_target_metadata_key_value,
                                                replace_field_on_target_schema)
from core.management.utils.xss_client import (
    get_required_fields_for_validation, get_source_validation_schema,
    get_target_metadata_for_transformation)
from core.models import MetadataLedger, SupplementalLedger

logger = logging.getLogger('dict_config_logger')


def get_source_metadata_for_transformation():
    """Retrieving Source metadata from MetadataLedger that needs to be
        transformed"""
    logger.info(
        "Retrieving source metadata from MetadataLedger to be transformed")
    source_data_dict = MetadataLedger.objects.values(
        'source_metadata').filter(
        source_metadata_validation_status='Y',
        record_lifecycle_status='Active').exclude(
        source_metadata_validation_date=None)

    return source_data_dict


def create_supplemental_metadata(metadata_columns, supplemental_metadata):
    """Function to identify supplemental metadata store them"""

    for metadata_column_list in metadata_columns:
        for column in metadata_column_list:
            supplemental_metadata.pop(column, None)
    return supplemental_metadata


def create_target_metadata_dict(target_mapping_dict, source_metadata,
                                required_column_list):
    """Function to replace and transform source data to target data for
    using target mapping schema"""

    # Create dataframe using target metadata schema
    target_schema = pd.DataFrame.from_dict(
        target_mapping_dict,
        orient='index')

    # Flatten source data dictionary for replacing and transformation
    source_metadata = dict_flatten(source_metadata, required_column_list)

    # Updating null values with empty strings for replacing metadata
    source_metadata = {
        k: '' if not v else v for k, v in
        source_metadata.items()}
    # assigning flattened source data
    metadata = dict_flatten(source_metadata, required_column_list)

    # send values to be skipped while creating supplemental data
    supplemental_metadata = \
        create_supplemental_metadata(target_schema.values.tolist(), metadata)

    # Replacing metadata schema with mapped values from source metadata
    target_schema = target_schema.replace(source_metadata)

    # Dropping index value and creating json object
    target_data = target_schema.apply(lambda x: [x.dropna()],
                                      axis=1).to_json()

    # Creating dataframe from json object
    target_data_df = pd.read_json(target_data)

    # transforming target dataframe to dictionary object for replacing
    # values in target with new value
    target_data_dict = target_data_df.to_dict(orient='index')

    return target_data_dict, supplemental_metadata


def store_transformed_source_metadata(key_value, key_value_hash,
                                      target_data_dict,
                                      hash_value, supplemental_metadata):
    """Storing target metadata in MetadataLedger"""
    data_for_transformation = MetadataLedger.objects.filter(
        source_metadata_key=key_value,
        record_lifecycle_status='Active',
        source_metadata_validation_status='Y'
    )

    if data_for_transformation.values("target_metadata_hash") != hash_value:
        data_for_transformation.update(target_metadata_validation_status='')

    data_for_transformation.update(
        source_metadata_transformation_date=timezone.now(),
        target_metadata_key=key_value,
        target_metadata_key_hash=key_value_hash,
        target_metadata=target_data_dict,
        target_metadata_hash=hash_value)

    # check if metadata has corresponding supplemental values and store
    if supplemental_metadata:
        source_extraction_date = MetadataLedger.objects.values_list(
            "source_metadata_extraction_date", flat=True).get(
            source_metadata_key=key_value,
            record_lifecycle_status='Active',
            source_metadata_validation_status='Y')

        transformation_date = MetadataLedger.objects.values_list(
            "source_metadata_transformation_date", flat=True).get(
            source_metadata_key=key_value,
            record_lifecycle_status='Active',
            source_metadata_validation_status='Y')

        SupplementalLedger.objects.get_or_create(
            supplemental_metadata_hash=hash_value,
            supplemental_metadata_key=key_value,
            supplemental_metadata_key_hash=key_value_hash,
            supplemental_metadata_transformation_date=transformation_date,
            supplemental_metadata_extraction_date=source_extraction_date,
            supplemental_metadata=supplemental_metadata,
            record_lifecycle_status='Active')


def transform_source_using_key(source_data_dict, target_mapping_dict,
                               required_column_list):
    """Transforming source data using target metadata schema"""
    logger.info(
        "Transforming source data using target renaming and mapping "
        "schemas and storing in json format ")
    logger.info("Identifying supplemental data and storing them ")
    len_source_metadata = len(source_data_dict)
    for ind in range(len_source_metadata):
        for table_column_name in source_data_dict[ind]:

            target_data_dict, supplemental_metadata = \
                create_target_metadata_dict(target_mapping_dict,
                                            source_data_dict
                                            [ind]
                                            [table_column_name],
                                            required_column_list
                                            )
            # Looping through target values in dictionary
            for ind1 in target_data_dict:
                # Replacing values in field referring target schema
                replace_field_on_target_schema(ind1,
                                               target_data_dict)
                # Key creation for target metadata
                key = get_target_metadata_key_value(target_data_dict[ind1])

                hash_value = hashlib.md5(
                    str(target_data_dict[ind1]).encode(
                        'utf-8')).hexdigest()
                store_transformed_source_metadata(key['key_value'],
                                                  key[
                                                      'key_value_hash'],
                                                  target_data_dict[
                                                      ind1],
                                                  hash_value,
                                                  supplemental_metadata)


class Command(BaseCommand):
    """Django command to extract data in the Experience index Agent (XIA)"""

    def handle(self, *args, **options):
        """
            Metadata is transformed in the XIA and stored in Metadata Ledger
        """
        target_mapping_dict = get_target_metadata_for_transformation()
        source_data_dict = get_source_metadata_for_transformation()
        schema_data_dict = get_source_validation_schema()
        required_column_list, recommended_column_list = \
            get_required_fields_for_validation(schema_data_dict)
        transform_source_using_key(source_data_dict, target_mapping_dict,
                                   required_column_list)

        logger.info('MetadataLedger updated with transformed data in XIA')
