import logging
from unittest.mock import patch

from django.test import tag

from core.tasks import execute_xia_automated_workflow

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class TasksTests(TestSetUp):

    @patch("core.tasks.execute_xia_automated_workflow.run")
    def test_execute_xia_automated_workflow(self, mock_run):
        """Testing the working of xia workflow celery task queue"""
        self.assert_(execute_xia_automated_workflow.run())

        self.assert_(execute_xia_automated_workflow.run())
        self.assertEqual(mock_run.call_count, 2)

        self.assert_(execute_xia_automated_workflow.run())
        self.assertEqual(mock_run.call_count, 3)
