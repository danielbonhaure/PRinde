from apscheduler.executors.pool import ThreadPoolExecutor
import time
from core.lib.jobs.scheduler import MonitoringScheduler
from core.modules.statistics.StatsCenter import StatsCenter
from test.mock import TestJob

__author__ = 'Federico Schmidt'


import unittest


class TestStatsCenter(unittest.TestCase):

    def setUp(self):
        self.scheduler = MonitoringScheduler(excecutors={
            'default': ThreadPoolExecutor(max_workers=2)
        }, job_defaults={
            'coalesce': True,
            'misfire_grace_time': 23*60*60  # 23 hours of grace time
        })
        self.stats = StatsCenter(self.scheduler)
        self.scheduler.start()

    def test_add_job(self):
        a_job = TestJob(ret_val=1)

        self.scheduler.add_job(a_job, 'A job', trigger='interval', seconds=3)
        # time.sleep(1)
        self.assertEqual(len(self.stats.tasks.keys()), 1)
