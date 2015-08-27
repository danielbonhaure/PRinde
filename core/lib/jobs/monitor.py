from abc import abstractmethod

__author__ = 'Federico Schmidt'

import time
from datetime import datetime
import copy

TASK_STARTED = 1
TASK_UPDATED = 2
TASK_ENDED = 4
EVENT_ALL = (TASK_STARTED | TASK_UPDATED | TASK_ENDED)


class ProgressObserver:
    @abstractmethod
    def progress_changed(self, event):
        pass


class ProgressEvent:
    def __init__(self, event_code, job, start_time, start_value, end_value, cur_val):
        self.event_type = event_code
        self.start_time = datetime.fromtimestamp(start_time)
        self.start_value = start_value
        self.end_value = end_value
        self.current_value = cur_val
        self.job = {'id': job.id, 'name': job.name}

    def serialize(self):
        d = copy.copy(self.__dict__)
        d['start_time'] = d['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        return d


class ProgressMonitor:
    def __init__(self, job=None, start_value=0, end_value=100):
        self.start_time = time.time()
        self.start_value = start_value
        self.end_value = end_value
        self.current_value = None
        self._progress_updates = []
        self._progress_listeners = []
        self.job = job

    def task_started(self):
        self.start_time = time.time()
        self._set_progress(self.start_value, TASK_STARTED)

    def update_progress(self, new_value):
        if new_value < self.start_value:
            raise RuntimeError('Invalid progress value (%s < %s).' % (new_value, self.start_value))
        if new_value > self.end_value:
            raise RuntimeError('Invalid progress value (%s > %s).' % (new_value, self.start_value))
        self._set_progress(new_value, TASK_UPDATED)

    def task_ended(self):
        self._set_progress(self.end_value, TASK_ENDED)

    def _set_progress(self, val, event_type):
        self.current_value = val
        self._progress_updates.append({'time': time.time() - self.start_time, 'value': val})

        e = ProgressEvent(event_type, self.job, self.start_time, self.start_value, self.end_value, val)

        # Notify observers.
        for l in self._progress_listeners:
            mask = l['mask']
            if mask & event_type:
                l['listener'].progress_changed(e)

    def add_listener(self, l, mask=EVENT_ALL):
        self._progress_listeners.append({'mask': mask, 'listener': l})


class NullMonitor(ProgressMonitor):
    def __init__(self):
        ProgressMonitor.__init__(self)

    def task_started(self):
        pass

    def update_progress(self, new_value):
        pass

    def task_ended(self):
        pass

    def _set_progress(self, val, event_type):
        pass

    def add_listener(self, l, mask=EVENT_ALL):
        pass
