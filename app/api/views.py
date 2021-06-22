from celery.utils.collections import OrderedDict
from django.shortcuts import render
from core.tasks import xia_workflow
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response


# Create your views here.

@api_view(['GET'])
@permission_classes((permissions.AllowAny,))
def xia_workflow_api(request):

    print('XIA workflow api')
    result = xia_workflow()
    print("RESULT")
    print(result)
    # print(result.id)
    # result = OrderedDict()
    # result['result'] = 'taskid'
    # result['code'] = status.HTTP_200_OK
    # result['message'] = 'success'
    return Response(result, status=status.HTTP_200_OK)
