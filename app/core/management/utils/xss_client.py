import json
import logging

import boto3
from core.management.utils.xia_internal import dict_flatten
from core.models import XIAConfiguration

logger = logging.getLogger('dict_config_logger')


def get_aws_bucket_name():
    """function returns the source bucket name"""
    bucket_name = 'xiaschema'
    return bucket_name


def read_json_data(file_name):
    """setting file path for json files and ingesting as dictionary values """
    s3 = boto3.resource('s3')
    bucket_name = get_aws_bucket_name()

    json_path = s3.Object(bucket_name, file_name)
    json_content = json_path.get()['Body'].read().decode('utf-8')
    data_dict = json.loads(json_content)
    return data_dict


def get_source_validation_schema():
    """Retrieve source validation schema from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    source_validation_schema = xia_data.source_metadata_schema
    logger.info("Reading schema for validation")
    # Read source validation schema as dictionary
    schema_data_dict = read_json_data(source_validation_schema)
    return schema_data_dict


def get_target_validation_schema():
    """Retrieve target validation schema from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    target_validation_schema = xia_data.target_metadata_schema
    logger.info("Reading schema for validation")
    # Read source validation schema as dictionary
    schema_data_dict = read_json_data(target_validation_schema)
    return schema_data_dict


def get_required_fields_for_validation(schema_data_dict):
    """Creating list of fields which are Required & Recommended"""

    # Call function to flatten schema used for validation
    flattened_schema_dict = dict_flatten(schema_data_dict, [])

    # Declare list for required and recommended column names
    required_column_list = list()
    recommended_column_list = list()

    #  Adding values to required and recommended list based on schema
    for column, value in flattened_schema_dict.items():
        if value == "Required":
            required_column_list.append(column)
        elif value == "Recommended":
            recommended_column_list.append(column)

    # Returning required and recommended list for validation
    return required_column_list, recommended_column_list


def get_target_metadata_for_transformation():
    """Retrieve target metadata schema from XIA configuration """
    logger.info("Configuration of schemas and files")
    xia_data = XIAConfiguration.objects.first()
    target_metadata_schema = xia_data.source_target_mapping
    logger.info("Reading schema for transformation")
    # Read source transformation schema as dictionary
    target_mapping_dict = read_json_data(target_metadata_schema)
    return target_mapping_dict
