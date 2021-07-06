import logging
import os

from celery import Celery

logger = logging.getLogger('dict_config_logger')

os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'openlxp_xia_jko_project.settings')

app = Celery('openlxp_xia_jko_project')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
