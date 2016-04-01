from apscheduler.schedulers.background import BackgroundScheduler

from core.lib.jobs.base import MonitoredFunctionJob, BaseJob
from core.lib.jobs.monitor import ProgressMonitor, EVENT_ALL

__author__ = 'Federico Schmidt'


class MonitoringScheduler(BackgroundScheduler):

    def __init__(self, *args, **kwargs):
        super(MonitoringScheduler, self).__init__(*args, **kwargs)
        self._progress_listeners = []

    def add_job(self, func, name=None, *args, **kwargs):
        """
        Adds a job to the scheduler. The func param can be either a function/method or a BaseJob instance.
        If it's a function, it'll be wrapped in a MonitoredFunctionJob in order to be traceable.

        :param func: A function or method to be called or a BaseJob instance.
        :return: An APScheduler Job instance.
        """
        if not isinstance(func, BaseJob):
            job = MonitoredFunctionJob(function=func, progress_monitor=ProgressMonitor())
        else:
            job = func
        # Add the wrapped job to the real scheduler.
        j = BackgroundScheduler.add_job(self, job.start, name=(name or job.name), *args, **kwargs)
        job.id = j.id
        job.name = j.name

        for l in self._progress_listeners:
            job.progress_monitor.add_listener(l['listener'], mask=l['mask'])

        return j

    def add_progress_listener(self, l, mask=EVENT_ALL):
        self._progress_listeners.append({'mask': mask, 'listener': l})
