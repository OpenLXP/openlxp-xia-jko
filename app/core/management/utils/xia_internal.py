import logging

logger = logging.getLogger('dict_config_logger')


def get_source_metadata_key_value(field_name, data_dict):
    """Function to create key value for source metadata """

    if field_name == 'LearningResourceIdentifier' or 'SOURCESYSTEM':
        key_course = data_dict.get('LearningResourceIdentifier')
        key_source = data_dict.get('SOURCESYSTEM')
        key_value = '_'.join([key_source, str(key_course)])

    return key_value


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
                return key_value
