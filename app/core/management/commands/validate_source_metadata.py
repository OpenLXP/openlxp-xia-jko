import logging
from django.core.management.base import BaseCommand
from core.models import XIAConfiguration
from core.models import MetadataLedger
from core.management.utils.xss_client import read_json_data
from core.management.utils.xia_internal import get_source_metadata_key_value
from django.utils import timezone

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

    for temp_k, temp_v in schema_data_dict.items():
        if temp_v == 'Required':
            required_column_name.append(temp_k)

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


def validate_source_using_key(source_data_dict, required_column_name):
    """Validating source data against required column names"""

    logger.info("Validating source data against required column names")
    len_source_metadata = len(source_data_dict)
    for ind in range(len_source_metadata):
        # Updating default validation for all records
        validation_result = 'Y'
        record_status_result = 'Active'
        for table_column_name in source_data_dict[ind]:
            for column in source_data_dict[ind][table_column_name]:
                if column in required_column_name:
                    # Key creation for source metadata
                    key = \
                        get_source_metadata_key_value(column,
                                                      source_data_dict[ind]
                                                      [table_column_name])

                    # Checking if value present in required fields
                    if not source_data_dict[ind][table_column_name][column]:
                        logger.error(
                            "Record " + str(
                                ind) + " does not have all REQUIRED "
                                       "fields. "
                            + column + " field is empty")
                        # Update validation result if not validated
                        validation_result = 'N'
                        record_status_result = 'Inactive'
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
        required_column_name = get_required_fields_for_source_validation\
            (schema_data_dict)
        source_data_dict = get_source_metadata_for_validation()
        validate_source_using_key(source_data_dict, required_column_name)

        logger.info(
            'MetadataLedger updated with source metadata validation status')
