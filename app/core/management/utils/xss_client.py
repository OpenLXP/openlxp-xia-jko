import json
import logging

import boto3

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
