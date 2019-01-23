from abc import abstractmethod
import time
from datetime import datetime
import copy

__author__ = 'Federico Schmidt'

JOB_STARTED = 1
JOB_UPDATED = 2
JOB_ENDED = 4
SUBJOB_ADDED = 8
SUBJOB_UPDATED = 16
SUBJOB_ENDED = 32
EVENT_ALL = (JOB_STARTED | JOB_UPDATED | JOB_ENDED | SUBJOB_ADDED | SUBJOB_UPDATED | SUBJOB_ENDED)
EVENT_SUBJOB = (SUBJOB_ADDED | SUBJOB_UPDATED | SUBJOB_ENDED)

JOB_STATUS_RUNNING = 1
JOB_STATUS_WAITING = 2
JOB_STATUS_FINISHED = 3
JOB_STATUS_ERROR = 4
JOB_STATUS_INACTIVE = 5
JOB_STATUS_RESCHEDULED = 6


class ProgressObserver(object):
    @abstractmethod
    def progress_changed(self, event):
        pass


class ProgressEvent(object):
    def __init__(self, event_code, job, start_time, start_value, end_value, job_status, cur_val):
        self.event_type = event_code
        self.start_time = datetime.fromtimestamp(start_time)
        self.start_value = start_value
        self.end_value = end_value
        self.current_value = cur_val
        self.job = {'id': job.id, 'name': job.name, 'parent': job.parent_job, 'status': job_status}

        if event_code & JOB_ENDED:
            self.end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def serialize(self):
        d = copy.copy(self.__dict__)
        d['start_time'] = d['start_time'].strftime('%Y-%m-%d %H:%M:%S')
        return d


class SubJobEvent(ProgressEvent):
    def __init__(self, event_code, job, start_time, start_value, end_value, job_status, cur_val, sub_job):
        super(SubJobEvent, self).__init__(event_code, job, start_time, start_value, end_value, job_status, cur_val)
        self.sub_job = {'id': sub_job.id, 'name': sub_job.name, 'parent': sub_job.parent_job}

    def serialize(self):
        d = ProgressEvent.serialize(self)
        d['sub_job'] = self.sub_job


class ProgressMonitor(ProgressObserver):

    def __init__(self, job=None, start_value=0, end_value=100):
        self.start_time = time.time()
        self.start_value = start_value
        self.end_value = end_value
        self.current_value = start_value
        self._progress_updates = []
        self._progress_listeners = []
        self.job = job
        self.job_status = JOB_STATUS_INACTIVE
        self.sub_jobs = {}

    def job_started(self, initial_status=None):
        if not initial_status:
            initial_status = JOB_STATUS_RUNNING
        self.start_time = time.time()
        self.job_status = initial_status
        self._set_progress(self.start_value, JOB_STARTED)

    def update_progress(self, new_value=None, job_status=JOB_STATUS_RUNNING):
        if not new_value:
            new_value = self.current_value

        # Check if there's anything that has really changed.
        if new_value == self.current_value and self.job_status == job_status:
            return

        # Validate the new progress value.
        if new_value < self.start_value:
            raise RuntimeError('Invalid progress value (%s < %s).' % (new_value, self.start_value))
        if new_value > self.end_value:
            raise RuntimeError('Invalid progress value (%s > %s).' % (new_value, self.end_value))

        # Update progress and notify.
        self.job_status = job_status
        self._set_progress(new_value, JOB_UPDATED)

    def job_ended(self, end_status=None):
        end_status = self.job_status if not end_status else end_status
        if end_status != JOB_STATUS_ERROR and end_status != JOB_STATUS_RESCHEDULED:
            end_status = JOB_STATUS_FINISHED
        self.job_status = end_status
        self._set_progress(self.end_value, JOB_ENDED)

    def add_subjob(self, subjob_progress_monitor, job_name=None):
        if not isinstance(subjob_progress_monitor, ProgressMonitor):
            raise RuntimeError('A subjob can only be added if a progress monitor is passed. Received type: %s.' %
                               type(subjob_progress_monitor))

        spm = subjob_progress_monitor

        if not spm.job:
            spm.job = copy.copy(self.job)
            if job_name:
                spm.job.name = job_name

        # If both jobs have the same ID, make sub job's ID different.
        if spm.job.id == self.job.id:
            spm.job.id = '%s_%s' % (self.job.id, len(self.sub_jobs))

        # Set parent ID.
        spm.job.parent_job = self.job.id

        # Add this object as listener.
        # When a subjob changes, progress_changed method will be called.
        spm.add_listener(self)
        self.sub_jobs[spm.job.id] = spm

        # Notify observers.
        e = ProgressEvent(SUBJOB_ADDED, spm.job, spm.start_time, spm.start_value, spm.end_value,
                          spm.job_status, spm.current_value)
        self._notify_observers(e)

    def progress_changed(self, event):
        """
        Progress observer for subjobs.
        :param event: The event emmited by a subjob's Progress Monitor.
        """
        if event.event_type & JOB_ENDED:
            del self.sub_jobs[event.job['id']]
            event.event_type = SUBJOB_ENDED
        else:
            event.event_type = SUBJOB_UPDATED
        self._notify_observers(event)

    def _set_progress(self, val, event_type):
        """
        Sets progress and notifies listeners.
        """
        self.current_value = val
        self._progress_updates.append({'time': time.time() - self.start_time, 'value': val})

        e = ProgressEvent(event_type, self.job, self.start_time, self.start_value, self.end_value, self.job_status, val)
        self._notify_observers(e)

    def _notify_observers(self, event):
        # Notify observers.
        for l in self._progress_listeners:
            mask = l['mask']
            if mask & event.event_type:
                l['listener'].progress_changed(event)

    def add_listener(self, l, mask=EVENT_ALL):
        for i, listener_tuple in enumerate(self._progress_listeners):
            msk = listener_tuple['mask']
            listener = listener_tuple['listener']
            # If we find the listener already registered do not add it again.
            if listener == l:
                if msk == mask:
                    return
                # Change the mask and return.
                self._progress_listeners[i]['mask'] = mask
                return
        self._progress_listeners.append({'mask': mask, 'listener': l})


class NullMonitor(ProgressMonitor):
    def __init__(self):
        ProgressMonitor.__init__(self)

    def job_started(self, initial_status=None):
        pass

    def update_progress(self, new_value=None, job_status=None):
        pass

    def job_ended(self, end_status=None):
        pass

    def add_subjob(self, subjob_progress_monitor,  job_name=None):
        pass

    def add_listener(self, l, mask=None):
        pass
