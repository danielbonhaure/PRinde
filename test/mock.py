import threading
import time
from core.lib.jobs.base import BaseJob

__author__ = 'Federico Schmidt'


class TestJob(BaseJob):
    def __init__(self, ret_val=0, *args, **kwargs):
        super(TestJob, self).__init__(*args, **kwargs)
        self.ret_val = ret_val

    def run(self):
        return self.ret_val


class ReaderThread(threading.Thread):

    def __init__(self, rwlock, id, run_order):
        super(ReaderThread, self).__init__()
        self.rwlock = rwlock
        self.id = id
        self.run_order = run_order

    def run(self):
        with self.rwlock.reader():
            time.sleep(0.5)
            self.run_order.append(self.id)


class WriterThread(threading.Thread):

    def __init__(self, rwlock, id, run_order, priority=0):
        super(WriterThread, self).__init__()
        self.rwlock = rwlock
        self.id = id
        self.run_order = run_order
        self.priority = priority

    def run(self):
        with self.rwlock.writer(self.priority):
            time.sleep(1)
            self.run_order.append(self.id)
