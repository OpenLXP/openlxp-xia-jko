from django.urls import path
from rest_framework.routers import DefaultRouter

from api import views

router = DefaultRouter()

app_name = 'api'

urlpatterns = [
    path('xia-workflow/', views.execute_xia_automated_workflow_api),
]
