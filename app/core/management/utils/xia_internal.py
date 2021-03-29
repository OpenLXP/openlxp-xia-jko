import hashlib
import logging

logger = logging.getLogger('dict_config_logger')


def get_key_dict(key_value, key_value_hash):
    """Creating key dictionary with all corresponding key values"""
    key = {'key_value': key_value, 'key_value_hash': key_value_hash}
    return key


def get_source_metadata_key_value(data_dict):
    """Function to create key value for source metadata """
    # field names depend on source data and SOURCESYSTEM is system generated
    field = ['LearningResourceIdentifier', 'SOURCESYSTEM']
    field_values = []

    for item in field:
        if not data_dict.get(item):
            logger.info('Field name ' + item + ' is missing for '
                                               'key creation')
        field_values.append(data_dict.get(item))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.md5(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key


def replace_field_on_target_schema(ind1,
                                   target_data_dict):
    """Replacing values in field referring target schema EducationalContext to
    course.MANDATORYTRAINING"""

    target_name = {
        "Course": [
            "EducationalContext",
        ]
    }
    for target_section_name in target_name:
        for target_field_name in target_name[target_section_name]:
            if target_data_dict[ind1][target_section_name]. \
                    get(target_field_name):

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
                                                 'Mandatory'


def get_target_metadata_key_value(data_dict):
    """Function to create key value for target metadata """
    field = {
        "Course": [
            "CourseCode",
            "CourseProviderName"
        ]
    }

    field_values = []

    for item_section in field:
        for item_name in field[item_section]:
            if not data_dict[item_section].get(item_name):
                logger.info('Field name ' + item_name + ' is missing for '
                                                        'key creation')
            field_values.append(data_dict[item_section].get(item_name))

    # Key value creation for source metadata
    key_value = '_'.join(field_values)

    # Key value hash creation for source metadata
    key_value_hash = hashlib.md5(key_value.encode('utf-8')).hexdigest()

    # Key dictionary creation for source metadata
    key = get_key_dict(key_value, key_value_hash)

    return key


def required_recommended_logs(id_num, category, field):
    """logs the missing required and recommended """

    # Logs the missing required columns
    if category == 'Required':
        logger.error(
            "Record " + str(
                id_num) + " does not have all " + category +
            " fields."
            + field + " field is empty")

    # Logs the missing recommended columns
    if category == 'Recommended':
        logger.warning(
            "Record " + str(
                id_num) + " does not have all " + category +
            " fields."
            + field + " field is empty")


def check_dict(ind, jsonObject, required_column_name, prefix,
               validation_result):
    """Navigates through dictionary for validation"""
    if isinstance(jsonObject[required_column_name[0]], dict):
        check_dict(ind, jsonObject[required_column_name[0]],
                   required_column_name[1:],
                   prefix + "." + required_column_name[0], validation_result)

    elif isinstance(jsonObject[required_column_name[0]], list):
        check_list(ind, jsonObject[required_column_name[0]],
                   required_column_name[1:],
                   prefix + "." + required_column_name[0], validation_result)

    elif isinstance(jsonObject[required_column_name[0]], str):
        check_validation_value(ind, jsonObject[required_column_name[0]],
                               required_column_name[0],
                               prefix + "." + required_column_name[0],
                               validation_result)


def check_list(ind, ele, required_column_name, prefix, validation_result):
    """Navigates through list for validation"""
    for i in range(len(ele)):
        if isinstance(ele[i][required_column_name[0]], list):

            check_list(ind, ele[i][required_column_name[0]],
                       required_column_name[1:],
                       prefix + "[" + str(i) + "]." + required_column_name[0],
                       validation_result)

        elif isinstance(ele[i][required_column_name[0]], str):

            check_validation_value(ind, ele[i][required_column_name[0]],
                                   required_column_name[0],
                                   prefix + "[" + str(i) + "]." +
                                   required_column_name[0],
                                   validation_result)
        else:
            check_dict(ind, ele[i][required_column_name[0]],
                       required_column_name[1:], prefix, validation_result)


def check_validation_value(ind, ele, required_column_name, prefix,
                           validation_result):
    """Checks value for validation return result and calls log function"""
    if not ele:
        required_recommended_logs(ind, "Required", prefix)
        validation_result = 'N'
    return validation_result
