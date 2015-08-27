from abc import abstractmethod
from datetime import datetime

from apscheduler.events import *

from lib.jobs.monitor import ProgressObserver, TASK_ENDED

__author__ = 'federico'


def job_evt_decorator(self):
    """
    Helper function to allow listening to events with an object's method.
    :return:
    """
    def process_event(e):
        self.job_event_listener(e)

    return process_event


def scheduler_evt_decorator(self):
    """
    Helper function to allow listening to events with an object's method.
    :return:
    """
    def process_event(e):
        self.scheduler_event_listener(e)

    return process_event


class StatEventListener:
    @abstractmethod
    def stat_event(self, event):
        pass


class StatsCenter(ProgressObserver):
    job_events = (EVENT_JOB_ADDED | EVENT_JOB_ERROR | EVENT_JOB_EXECUTED | EVENT_JOB_MISSED |
                  EVENT_JOB_MODIFIED | EVENT_JOB_REMOVED)
    scheduler_events = (EVENT_SCHEDULER_START | EVENT_SCHEDULER_SHUTDOWN)

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.tasks = {}
        self.running_tasks = {}
        self.finished_tasks = {}
        self.event_listeners = []

        if self.scheduler.running:
            self.init_jobs()

        # Add the stats center as an event listener, decorating the object's methods.
        scheduler.add_listener(job_evt_decorator(self), mask=self.job_events)
        scheduler.add_listener(scheduler_evt_decorator(self), mask=self.scheduler_events)

    def job_event_listener(self, event):
        if event.code & EVENT_JOB_ADDED:
            job = self.scheduler.get_job(event.job_id)
            now = datetime.now(self.scheduler.timezone)
            self.tasks[event.job_id] = {
                'job': StatsCenter.job_view(job),
                'next_run': job.trigger.get_next_fire_time(None, now).strftime('%Y-%m-%d %H:%M:%S')
            }
        elif event.code & EVENT_JOB_EXECUTED:
            self.finished_tasks[event.job_id] = self.tasks[event.job_id]
            self.finished_tasks[event.job_id]['event'] = StatsCenter.event_view(event,
                                                                                datetime.now(self.scheduler.timezone))
            # Check if the job was removed from the schedulers queue.
            # A job may be ran and never deleted (perdiodic jobs).
            if not self.scheduler.get_job(event.job_id):
                # If it was removed, delete it from active tasks.
                del self.tasks[event.job_id]

        # Notify observers.
        for listener in self.event_listeners:
            listener.stat_event(None)

    def scheduler_event_listener(self, event):
        if event.code & EVENT_SCHEDULER_START:
            self.init_jobs()
        elif event.code & EVENT_SCHEDULER_SHUTDOWN:
            pass
            # TODO:propagate system shutdown (or try to restart the scheduler if it was caused by an error?).

    def init_jobs(self):
        now = datetime.now(self.scheduler.timezone)
        self.tasks = {}
        for j in self.scheduler.get_jobs():
            self.tasks[j.id] = {
                'job': StatsCenter.job_view(j),
                'next_run': j.trigger.get_next_fire_time(None, now).strftime('%Y-%m-%d %H:%M:%S')
            }

    @staticmethod
    def job_view(j):
        return {
            'name': j.name,
            'id': j.id,
            'trigger': str(j.trigger)
        }

    @staticmethod
    def event_view(e, now):
        return {
            'retval': e.retval if e.retval else 0,
            'scheduled_run_time': e.scheduled_run_time,
            'end_time': now.strftime('%Y-%m-%d %H:%M:%S')
        }

    def progress_changed(self, event):
        # This listener should save only active tasks (those showing progress).
        if event.current_value == event.start_value:
            return

        job_id = event.job['id']
        if event.event_type & TASK_ENDED and job_id in self.running_tasks:
            del self.running_tasks[job_id]
        else:
            self.running_tasks[job_id] = event.serialize()
