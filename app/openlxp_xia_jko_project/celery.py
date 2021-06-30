import os
import logging
from celery import Celery
logger = logging.getLogger('dict_config_logger')

os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'openlxp_xia_jko_project.settings')

app = Celery('openlxp_xia_jko_project')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    logger.info('Request: {0!r}'.format(self.request))
