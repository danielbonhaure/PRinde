from abc import abstractmethod
import inspect
from datetime import datetime
from uuid import uuid4
import logging

from lib.jobs.monitor import NullMonitor
from lib.utils.log import log_format_exception

__author__ = 'Federico Schmidt'


class BaseJob(object):

    def __init__(self, name=None, id=None, progress_monitor=None):
        if not name:
            name = "Job created at %s" % datetime.now().isoformat()
        if not progress_monitor:
            progress_monitor = NullMonitor()
        self.id = (id if id else uuid4().hex)
        self.name = name
        self.progress_monitor = progress_monitor
        self.progress_monitor.job = self

    def start(self, *args, **kwargs):
        try:
            self.progress_monitor.task_started()
            ret_val = self.run(*args, **kwargs)
            if not ret_val:
                return 0
            return ret_val
        except Exception:
            logging.getLogger().error('An exception was raised while running Job "%s". Details: %s' %
                                      (self.name, log_format_exception()))
            return 1
        finally:
            self.progress_monitor.task_ended()

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class MonitoredFunctionJob(BaseJob):

    def __init__(self, function, *args, **kwargs):
        super(MonitoredFunctionJob, self).__init__(*args, **kwargs)
        self.callable = function

    def run(self, *args, **kwargs):
        arg_names = inspect.getargspec(self.callable)[0]
        if 'progress_monitor' in arg_names:
            # Replace progress monitor reference so the function can report progress.
            self.callable(progress_monitor=self.progress_monitor, *args, **kwargs)
        else:
            self.callable(*args, **kwargs)
