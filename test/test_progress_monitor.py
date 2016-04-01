from test.mock import TestJob
from core.lib.jobs.monitor import *

__author__ = 'Federico Schmidt'


import unittest


class TestProgressMonitor(unittest.TestCase):

    def setUp(self):
        self.events = []

    def tearDown(self):
        self.events = []

    def test_event_masks(self):
        j = TestJob(name='Job j', progress_monitor=ProgressMonitor())
        j.progress_monitor.add_listener(self, mask=(JOB_STARTED | JOB_ENDED))

        j.progress_monitor.job_started()
        j.progress_monitor.update_progress(new_value=50, job_status=JOB_STATUS_WAITING)
        j.progress_monitor.update_progress(job_status=JOB_STATUS_RUNNING)
        j.progress_monitor.job_ended()

        self.assertEqual(len(self.events), 2)  # Only JOB_STARTED and JOB_ENDED should be notified.

    def test_subtask_event_propagation(self):
        j = TestJob(name='Job j', progress_monitor=ProgressMonitor())
        k = TestJob(name='Job k', progress_monitor=ProgressMonitor())
        j.progress_monitor.add_listener(self)
        j.progress_monitor.add_subjob(k.progress_monitor)

        k.progress_monitor.update_progress(5, job_status=JOB_STATUS_WAITING)
        self.assertEqual(len(self.events), 2)
        self.assertEqual(self.events[0].event_type, SUBJOB_ADDED)
        self.assertEqual(self.events[0].job['parent'], j.id)
        self.assertEqual(self.events[1].event_type, SUBJOB_UPDATED)

    def test_listeners_are_unique(self):
        a_job = TestJob(ret_val=1, progress_monitor=ProgressMonitor())
        a_job.progress_monitor.add_listener(self)

        self.assertEqual(len(a_job.progress_monitor._progress_listeners), 1)
        self.assertEqual(a_job.start(), 1)  # Check that the returned value equals the one we gave the job on init.

        # Add the same listener again.
        a_job.progress_monitor.add_listener(self)
        self.assertEqual(len(a_job.progress_monitor._progress_listeners), 1)

        # Add the same listener again but with a different mask.
        a_job.progress_monitor.add_listener(self, mask=EVENT_SUBJOB)
        self.assertEqual(len(a_job.progress_monitor._progress_listeners), 1)
        # Assert that the mask changed.
        self.assertEqual(a_job.progress_monitor._progress_listeners[0]['mask'], EVENT_SUBJOB)

    def test_subtask_removed_on_finish(self):
        # Subtasks monitors should be removed from the parent task once they finish.
        parent_job = TestJob(name='Parent', progress_monitor=ProgressMonitor())
        child_job = TestJob(name='Child', progress_monitor=ProgressMonitor())
        parent_job.progress_monitor.add_listener(self)
        parent_job.progress_monitor.add_subjob(child_job.progress_monitor)

        sub_jobs_ids = parent_job.progress_monitor.sub_jobs.keys()
        self.assertEqual(len(sub_jobs_ids), 1)
        self.assertEqual(sub_jobs_ids[0], child_job.id)

        child_job.start()

        sub_jobs_ids = parent_job.progress_monitor.sub_jobs.keys()
        self.assertEqual(len(sub_jobs_ids), 0)

    def progress_changed(self, event):
        self.events.append(event)
