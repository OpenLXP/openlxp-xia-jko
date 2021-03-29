import logging

from core.management.utils.xia_internal import (get_target_metadata_key_value,
                                                required_recommended_logs)
from core.management.utils.xss_client import read_json_data
from core.models import MetadataLedger, XIAConfiguration
from django.core.management.base import BaseCommand
from django.utils import timezone

logger = logging.getLogger('dict_config_logger')


def get_target_validation_schema():
    """Retrieve target validation schema from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    target_validation_schema = xia_data.target_metadata_schema
    return target_validation_schema


def read_target_validation_schema(target_validation_schema):
    """Creating dictionary from schema"""
    logger.info("Reading schema for validation")
    # Read source validation schema as dictionary
    schema_data_dict = read_json_data(target_validation_schema)
    return schema_data_dict


def get_required_recommended_fields_for_target_validation(schema_data_dict):
    """Creating list of fields which are Required & Recommended"""
    required_dict = {}
    recommended_dict = {}

    # Getting key list whose Value is Required
    for k in schema_data_dict:
        required_list = []
        recommended_list = []
        for k1, v1 in schema_data_dict[k].items():
            if v1 == 'Required':
                required_list.append(k1)
            if v1 == 'Recommended':
                recommended_list.append(k1)

            required_dict[k] = required_list

            recommended_dict[k] = recommended_list

    return required_dict, recommended_dict


def get_target_metadata_for_validation():
    """Retrieving target metadata from MetadataLedger that needs to be
        validated"""
    logger.info(
        "Accessing target metadata from MetadataLedger that needs to be "
        "validated")
    target_data_dict = MetadataLedger.objects.values(
        'target_metadata').filter(target_metadata_validation_status='',
                                  record_lifecycle_status='Active'
                                  ).exclude(
        source_metadata_transformation_date=None)
    return target_data_dict


def store_target_metadata_validation_status(target_data_dict, key_value_hash,
                                            validation_result,
                                            record_status_result):
    """Storing validation result in MetadataLedger"""
    target_data_dict.filter(
        target_metadata_key_hash=key_value_hash).update(
        target_metadata_validation_status=validation_result,
        target_metadata_validation_date=timezone.now(),
        record_lifecycle_status=record_status_result,
        metadata_record_inactivation_date=timezone.now())


def validate_target_using_key(target_data_dict, required_dict,
                              recommended_dict):
    """Validating target data against required column names"""

    logger.info('Validating and updating records in MetadataLedger table')
    len_target_metadata = len(target_data_dict)
    for ind in range(len_target_metadata):
        for val in target_data_dict[ind]:
            validation_result = 'Y'
            record_status_result = 'Active'
            for column in target_data_dict[ind][val]:
                required_columns = required_dict[column]
                recommended_columns = recommended_dict[column]
                for key in target_data_dict[ind][val][column]:
                    if key in required_columns:
                        if not target_data_dict[ind][val][column][key]:
                            # logging missing required columns in records
                            required_recommended_logs(ind, "Required", column)
                            validation_result = 'N'
                            record_status_result = 'Inactive'
                    if key in recommended_columns:
                        if not target_data_dict[ind][val][column][key]:
                            # logging missing required columns in records
                            required_recommended_logs(ind, "Recommended",
                                                      column)
            # Key creation for target metadata
            key = \
                get_target_metadata_key_value(target_data_dict[ind][val])

            store_target_metadata_validation_status(
                target_data_dict, key['key_value_hash'],
                validation_result, record_status_result)


class Command(BaseCommand):
    """Django command to validate target data"""

    def handle(self, *args, **options):
        """
            target data is validated and stored in metadataLedger
        """
        target_validation_schema = get_target_validation_schema()
        schema_data_dict = \
            read_target_validation_schema(target_validation_schema)
        target_data_dict = get_target_metadata_for_validation()
        required_dict, recommended_dict = \
            get_required_recommended_fields_for_target_validation(
                schema_data_dict)
        validate_target_using_key(target_data_dict, required_dict,
                                  recommended_dict)
        logger.info(
            'MetadataLedger updated with target metadata validation status')
