import logging
import hashlib

logger = logging.getLogger('dict_config_logger')


def get_key_dict(key_value, key_value_hash):
    """Creating key dictionary with all corresponding key values"""
    key = {'key_value': key_value, 'key_value_hash': key_value_hash}
    return key


def get_source_metadata_key_value(field_name, data_dict):
    """Function to create key value for source metadata """

    if field_name == 'LearningResourceIdentifier' or 'SOURCESYSTEM':
        key_course = data_dict.get('LearningResourceIdentifier')
        key_source = data_dict.get('SOURCESYSTEM')
        key_value = '_'.join([key_source, str(key_course)])
        key_value_hash = hashlib.md5(key_value.encode('utf-8')).hexdigest()
        key = get_key_dict(key_value, key_value_hash)

    return key


def replace_field_on_target_schema(ind1, target_section_name,
                                   target_field_name,
                                   target_data_dict):
    """Replacing values in field referring target schema EducationalContext to
    course.MANDATORYTRAINING"""

    if target_field_name == 'EducationalContext':
        if target_data_dict[ind1][target_section_name][
            target_field_name] == 'y' or \
                target_data_dict[ind1][
                    target_section_name][
                    target_field_name] == 'Y':
            target_data_dict[ind1][
                target_section_name][
                target_field_name] = 'Mandatory'
        else:
            if target_data_dict[ind1][
                target_section_name][
                target_field_name] == 'n' or \
                    target_data_dict[ind1][
                        target_section_name][
                        target_field_name] == 'N':
                target_data_dict[ind1][
                    target_section_name][
                    target_field_name] = 'Non - ' \
                                         'Mandatory '


def get_target_metadata_key_value(field_name, data_dict):
    """Function to create key value for target metadata """

    # field names depend on target data schema
    if field_name == 'CourseCode' or \
            'CourseProviderName':
        key_course = data_dict.get(
            'CourseCode')
        key_source = data_dict.get(
            'CourseProviderName')
        if key_source:
            if key_course:
                key_value = '_'.join(
                    [key_source, key_course])
                key_value_hash = hashlib.md5(key_value.encode('utf-8')). \
                    hexdigest()
                key = get_key_dict(key_value, key_value_hash)
                return key

        key = get_key_dict(None, None)
        return key
