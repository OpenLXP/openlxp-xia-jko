import logging
from unittest.mock import patch

from django.test import tag

from core.tasks import xia_workflow

from .test_setup import TestSetUp

logger = logging.getLogger('dict_config_logger')


@tag('unit')
class TasksTests(TestSetUp):

    @patch("core.tasks.xia_workflow.run")
    def test_xia_workflow(self, mock_run):
        """Testing the working of xia workflow celery task queue"""
        self.assert_(xia_workflow.run())

        self.assert_(xia_workflow.run())
        self.assertEqual(mock_run.call_count, 2)

        self.assert_(xia_workflow.run())
        self.assertEqual(mock_run.call_count, 3)
