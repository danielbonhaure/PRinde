__author__ = 'Federico Schmidt'

from core.lib.jobs.monitor import *
from core.lib.jobs.base import BaseJob

import unittest


class TestProgressMonitor(unittest.TestCase):

    def setUp(self):
        self.events = []

    def tearDown(self):
        self.events = []

    def test_event_masks(self):
        j = BaseJob(name='Job j', progress_monitor=ProgressMonitor())
        j.progress_monitor.add_listener(self, mask=(JOB_STARTED | JOB_ENDED))

        j.progress_monitor.job_started()
        j.progress_monitor.update_progress(new_value=50, job_status=JOB_STATUS_WAITING)
        j.progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)
        j.progress_monitor.job_ended()

        self.assertEqual(len(self.events), 2)  # Only JOB_STARTED and JOB_ENDED should be notified.

    def test_subtask_event_propagation(self):
        j = BaseJob(name='Job j', progress_monitor=ProgressMonitor())
        k = BaseJob(name='Job k', progress_monitor=ProgressMonitor())
        j.progress_monitor.add_listener(self)
        j.progress_monitor.add_subjob(k.progress_monitor)

        k.progress_monitor.update_progress(5, job_status=JOB_STATUS_WAITING)
        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0].event_type, SUBJOB_ADDED)
        self.assertEqual(self.events[0].job['parent'], j.id)
        self.assertEqual(self.events[1].event_type, SUBJOB_UPDATED)

    def progress_changed(self, event):
        self.events.append(event)
