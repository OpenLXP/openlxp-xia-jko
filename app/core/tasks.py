from celery import shared_task
import logging
from core.management.commands.extract_source_metadata import Command as \
    extract_Command
from core.management.commands.validate_source_metadata import Command as \
    validate_source_Command
from core.management.commands.transform_source_metadata import Command as \
    transform_Command
from core.management.commands.validate_target_metadata import Command as \
    validate_target_Command
from core.management.commands.load_target_metadata import Command as \
    load_Command
from core.management.commands.conformance_alerts import Command as \
    conformance_alerts_Command
logger = logging.getLogger('dict_config_logger')


@shared_task
def xia_workflow():
    """XIA automated workflow"""

    extract_class = extract_Command()
    validate_source_class = validate_source_Command()
    transform_class = transform_Command()
    validate_target_class = validate_target_Command()
    load_class = load_Command()
    conformance_alerts_class = conformance_alerts_Command()

    extract_class.handle()
    # validate_source_class.handle()
    # transform_class.handle()
    # validate_target_class.handle()
    # load_class.handle()
    # conformance_alerts_class.handle()

    logger.info('COMPLETED WORKFLOW')
