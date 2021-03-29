import logging

from django.core.management.base import BaseCommand
from django.utils import timezone

from core.management.utils.xia_internal import (get_source_metadata_key_value,
                                                required_recommended_logs,
                                                check_dict, check_list,
                                                check_validation_value)
from core.management.utils.xss_client import read_json_data
from core.models import MetadataLedger, XIAConfiguration

logger = logging.getLogger('dict_config_logger')


def get_source_validation_schema():
    """Retrieve source validation schema from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    source_validation_schema = xia_data.source_metadata_schema
    logger.info("Reading schema for validation")
    # Read source validation schema as dictionary
    schema_data_dict = read_json_data(source_validation_schema)
    return schema_data_dict


def get_required_fields_for_source_validation(schema_data_dict):
    """Creating list of fields which are Required"""
    required_column_name = list()
    for element in schema_data_dict:
        # creates required_column_list using path of keys to element required
        if schema_data_dict[element] == "Required":
            required_column_name.append(element)
    return required_column_name


def get_source_metadata_for_validation():
    """Retrieving  source metadata from MetadataLedger that needs to be
        validated"""
    logger.info(
        "Accessing source metadata from MetadataLedger that needs to be "
        "validated")
    source_data_dict = MetadataLedger.objects.values(
        'source_metadata').filter(source_metadata_validation_status='',
                                  record_lifecycle_status='Active'
                                  ).exclude(
        source_metadata_extraction_date=None)

    return source_data_dict


def store_source_metadata_validation_status(source_data_dict,
                                            key_value_hash, validation_result,
                                            record_status_result):
    """Storing validation result in MetadataLedger"""

    source_data_dict.filter(
        source_metadata_key_hash=key_value_hash).update(
        source_metadata_validation_status=validation_result,
        source_metadata_validation_date=timezone.now(),
        record_lifecycle_status=record_status_result,
        metadata_record_inactivation_date=timezone.now()
    )


def source_metadata_value_for_validation(
        ind, data_dict, required_column_name_list, validation_result):
    """function to navigate to value in source metadata to be validated """
    # If data value is a dictionary
    if isinstance(data_dict[required_column_name_list[0]], dict):
        check_dict(ind, data_dict[required_column_name_list[0]],
                   required_column_name_list[1:], required_column_name_list[0],
                   validation_result)

    # If data is a list
    elif isinstance(data_dict[required_column_name_list[0]], list):
        check_list(ind, data_dict[required_column_name_list[0]],
                   required_column_name_list[1:], required_column_name_list[0],
                   validation_result)

    # If data value is a string or NoneType
    else:
        validation_result =\
            check_validation_value(ind,
                                   data_dict[required_column_name_list[0]],
                                   required_column_name_list[0],
                                   required_column_name_list[0],
                                   validation_result)

    return validation_result


def validate_source_using_key(source_data_dict, required_column_name):
    """Validating source data against required column names"""

    logger.info("Validating source data against required column names")
    len_source_metadata = len(source_data_dict)
    for ind in range(len_source_metadata):
        # Updating default validation for all records
        validation_result = 'Y'
        record_status_result = 'Active'
        for table_column_name in source_data_dict[ind]:
            # looping in source metadata
            for item in required_column_name:
                # looping through elements in required column list
                data_dict = source_data_dict[ind][table_column_name]
                # creating list of key values to access required value split
                # by .
                item_list = item.split('.')
                # function to navigate to required values and validate
                validation_result = \
                    source_metadata_value_for_validation(
                        ind, data_dict, item_list,
                        validation_result)

            # if validation fails for record record status is
            # Update to record status to inactive if record is invalid
            if validation_result == 'N':
                record_status_result = 'Inactive'

            # Key creation for source metadata
            key = \
                get_source_metadata_key_value(source_data_dict[ind]
                                              [table_column_name])
        # Calling function to update validation status
        store_source_metadata_validation_status(source_data_dict,
                                                key['key_value_hash'],
                                                validation_result,
                                                record_status_result)


class Command(BaseCommand):
    """Django command to validate source data"""

    def handle(self, *args, **options):
        """
            Source data is validated and stored in metadataLedger
        """
        schema_data_dict = get_source_validation_schema()
        required_column_name = \
            get_required_fields_for_source_validation(schema_data_dict)
        source_data_dict = get_source_metadata_for_validation()
        validate_source_using_key(source_data_dict, required_column_name)

        logger.info(
            'MetadataLedger updated with source metadata validation status')
