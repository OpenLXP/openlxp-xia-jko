import logging

from celery.result import AsyncResult
from django.http import JsonResponse
from rest_framework import permissions
from rest_framework.decorators import api_view, permission_classes

from core.tasks import execute_xia_automated_workflow

logger = logging.getLogger('dict_config_logger')


@api_view(['GET'])
@permission_classes((permissions.AllowAny,))
def execute_xia_automated_workflow_api(request):
    logger.info('XIA workflow api')
    task = execute_xia_automated_workflow.delay()
    return JsonResponse({"task_id": task.id}, status=202)


def get_status(request, task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JsonResponse(result, status=200)
