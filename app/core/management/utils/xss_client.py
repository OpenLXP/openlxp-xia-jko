import logging
import boto3
import json


logger = logging.getLogger('dict_config_logger')


def aws_get():
    """function to accept aws bucket name and source schema file name"""
    bucket_name = 'jkoxia'
    return bucket_name


def read_json_data(file_name):
    """setting file path for json files and ingesting as dictionary values """
    s3 = boto3.resource('s3')
    bucket_name = aws_get()

    json_path = s3.Object(bucket_name, file_name)
    json_content = json_path.get()['Body'].read().decode('utf-8')
    data_dict = json.loads(json_content)
    return data_dict

