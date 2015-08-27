__author__ = 'Federico Schmidt'

from contextlib import contextmanager
from threading import RLock, Semaphore


class PrioritizedRWLock(object):
    """
    Read-write lock. Writers go first, readers can read all at the same time.
    Writers access order is determined by priority.

    Based on: https://hdknr.github.io/docs/django/modules/django/utils/synch.html#RWLock
    """
    def __init__(self):
        self.inner_lock = RLock()
        self.read_lock = Semaphore(0)
        self.active_readers = 0
        self.waiting_readers = 0
        self.active_writers = 0
        self.queued_writers = dict()

    def acquire_read(self):
        with self.inner_lock:
            if self.active_writers == 0 and len(self.queued_writers) == 0:
                self.active_readers += 1
                self.read_lock.release()
            else:
                self.waiting_readers += 1
        self.read_lock.acquire()

    def acquire_write(self, priority=0):
        self.inner_lock.acquire()
        # Check if a write lock can be acquired.
        if self.active_writers == 0 and len(self.queued_writers) == 0 and self.active_readers == 0:
            self.active_writers += 1
            self.inner_lock.release()
        else:
            # Queue writer with it's own lock.
            new_lock = Semaphore(0)
            # Enqueue writer.
            if priority in self.queued_writers:
                self.queued_writers[priority].append(new_lock)
            else:
                self.queued_writers[priority] = [new_lock]
            # We must release the inner lock before locking the writer, otherwise we could end up with a deadlock.
            self.inner_lock.release()
            # Calling new_lock.acquire() will lock the writer.
            new_lock.acquire()

    def release_read(self):
        with self.inner_lock:
            self.active_readers -= 1
            if self.active_readers == 0 and len(self.queued_writers) != 0:
                self.__unlock_writer__()

    def release_write(self):
        with self.inner_lock:
            self.active_writers -= 1
            if len(self.queued_writers) != 0:
                # Writers go first, unlock the most prioritized writer.
                self.__unlock_writer__()
            elif self.waiting_readers != 0:
                # Release readers lock.
                for i in range(0, self.waiting_readers):
                    self.read_lock.release()
                # Update active and waiting readers count.
                self.active_readers += self.waiting_readers
                self.waiting_readers = 0

    def __unlock_writer__(self):
        """
        Releases the lock on the writer with the highest priority inside the queued_writers dict.

        Priority is determined by sorting the dictionary's keys in descending order. If more than one writer with max
         priority is found, the order is FIFO.
        """
        self.active_writers += 1
        # Release a writer inside the highest priority group.
        highest_priority = sorted(self.queued_writers, reverse=True)[0]
        self.queued_writers[highest_priority].pop(0).release()

        if len(self.queued_writers[highest_priority]) == 0:
            # Remove the highest priority section for it has no more writers waiting.
            del self.queued_writers[highest_priority]

    @contextmanager
    def reader(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def writer(self, priority=0):
        self.acquire_write(priority)
        try:
            yield
        finally:
            self.release_write()


class JobsLock(PrioritizedRWLock):
    """
    A lock that limits the amount of parallel readers and create aliases for the reader and writer context managers.
    Ideal for controlling blocking tasks (writers) and parallel tasks (readers) with a maximum degree of parallelism.
    """

    def __init__(self, max_parallel_tasks=1):
        super(JobsLock, self).__init__()
        self.max_concurrent_readers = max_parallel_tasks

    def acquire_read(self):
        """
        Readers (non blocking jobs) are restricted to a max parallel amount.
        :return:
        """
        with self.inner_lock:
            if self.active_writers == 0 and len(self.queued_writers) == 0 and \
               self.active_readers < self.max_concurrent_readers:
                self.active_readers += 1
                self.read_lock.release()
            else:
                self.waiting_readers += 1
        self.read_lock.acquire()

    def release_write(self):
        with self.inner_lock:
            self.active_writers -= 1
            if len(self.queued_writers) != 0:
                # Writers go first, unlock the most prioritized writer.
                self.__unlock_writer__()
            elif self.waiting_readers != 0:
                # Release readers lock.
                for i in range(0, min(self.waiting_readers, self.max_concurrent_readers)):
                    self.read_lock.release()
                    # Update active and waiting readers count.
                    self.active_readers += 1
                    self.waiting_readers -= 1


    # Create aliases for the reader and writer context managers.
    def parallel_job(self):
        return self.reader

    def blocking_job(self):
        return self.writer
