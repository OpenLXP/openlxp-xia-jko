import os

from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE',
                      'openlxp_xia_jko_project.settings')

app = Celery('openlxp_xia_jko_project')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# app.conf.beat_schedule = {
#     'add-every-minute-contrab': {
#         'task': 'workflow_for_xia',
#         'schedule': crontab(minute='*/3'),
#         # 'args': (16, 16),
#     },
#     'add-every-5-seconds': {
#         'task': 'multiply_two_numbers',
#         'schedule': 5.0,
#         'args': (16, 16)
#     },
#     'add-every-30-seconds': {
#         'task': 'tasks.add',
#         'schedule': 30.0,
#         'args': (16, 16)
#     },
# }


@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
