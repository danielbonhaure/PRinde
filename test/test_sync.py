import threading
import time
import unittest
from core.lib.utils.sync import PrioritizedRWLock, JobsLock

__author__ = 'Federico Schmidt'

class Reader(threading.Thread):

    def __init__(self, rwlock, id, run_order):
        super(Reader, self).__init__()
        self.rwlock = rwlock
        self.id = id
        self.run_order = run_order

    def run(self):
        with self.rwlock.reader():
            time.sleep(0.5)
            self.run_order.append(self.id)


class Writer(threading.Thread):

    def __init__(self, rwlock, id, run_order, priority=0):
        super(Writer, self).__init__()
        self.rwlock = rwlock
        self.id = id
        self.run_order = run_order
        self.priority = priority

    def run(self):
        with self.rwlock.writer(self.priority):
            time.sleep(1)
            self.run_order.append(self.id)


class TestPrioritizedRWLock(unittest.TestCase):

    def test_readers_run_parallel(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        for i in range(0, 5):
            r = Reader(lock, i, run_order)
            _threads.append(r)

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertGreaterEqual(lock.active_readers, 2)

        for t in _threads:
            t.join()

    def test_writer_blocks_readers(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        w = Writer(lock, "writer", run_order)
        _threads.append(w)

        for i in range(0, 5):
            r = Reader(lock, i, run_order)
            _threads.append(r)

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertEqual(lock.waiting_readers, 5)
        self.assertEqual(lock.active_writers, 1)

        time.sleep(1)
        self.assertEqual(lock.waiting_readers, 0)

        for t in _threads:
            t.join()
        self.assertEqual(run_order[0], "writer")

    def test_writers_go_first(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        r1 = Reader(lock, "reader 1", run_order)
        w = Writer(lock, "writer", run_order)
        r2 = Reader(lock, "reader 2", run_order)

        _threads.append(r1)
        _threads.append(w)
        _threads.append(r2)

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertEqual(lock.active_readers, 1)
        self.assertEqual(len(lock.queued_writers), 1)
        self.assertEqual(lock.waiting_readers, 1)

        time.sleep(0.5)
        self.assertEqual(lock.waiting_readers, 1)
        self.assertEqual(len(lock.queued_writers), 0)
        self.assertEqual(lock.active_writers, 1)

        for t in _threads:
            t.join()
        self.assertEqual(run_order, ["reader 1", "writer", "reader 2"])

    def test_writers_priority(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        r1 = Reader(lock, "reader 1", run_order)
        w1 = Writer(lock, "writer 1", run_order, priority=0)
        r2 = Reader(lock, "reader 2", run_order)
        w2 = Writer(lock, "writer 2", run_order, priority=1)  # This writer should run first.

        _threads.append(r1)
        _threads.append(w1)
        _threads.append(r2)
        _threads.append(w2)

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertEqual(lock.active_readers, 1)
        self.assertEqual(len(lock.queued_writers), 2)
        self.assertEqual(lock.waiting_readers, 1)

        for t in _threads:
            t.join()

        self.assertEqual(run_order, ["reader 1", "writer 2", "writer 1", "reader 2"])

    def test_jobs_lock(self):
        lock = JobsLock()
        _threads = []
        run_order = []

        r1 = Reader(lock, "reader 1", run_order)
        w1 = Writer(lock, "writer 1", run_order, priority=0)
        r2 = Reader(lock, "reader 2", run_order)
        w2 = Writer(lock, "writer 2", run_order, priority=1)  # This writer should run first.

        _threads.append(r1)
        _threads.append(w1)
        _threads.append(r2)
        _threads.append(w2)

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertEqual(lock.active_readers, 1)
        self.assertEqual(len(lock.queued_writers), 2)
        self.assertEqual(lock.waiting_readers, 1)

        for t in _threads:
            t.join()

        self.assertEqual(run_order, ["reader 1", "writer 2", "writer 1", "reader 2"])

    def test_jobs_lock_parallelism(self):
        lock = JobsLock(max_parallel_tasks=2)
        _threads = []
        run_order = []

        _threads.append(Reader(lock, "reader 1", run_order))
        _threads.append(Writer(lock, "writer 1", run_order))
        _threads.append(Reader(lock, "reader 2", run_order))
        _threads.append(Reader(lock, "reader 3", run_order))

        for t in _threads:
            t.start()

        time.sleep(0.2)
        self.assertEqual(lock.active_readers, 1)
        self.assertEqual(len(lock.queued_writers), 1)
        self.assertEqual(lock.waiting_readers, 2)

        time.sleep(0.5)
        # Once the reader finishes, the writer should run.
        self.assertEqual(lock.active_writers, 1)
        self.assertEqual(lock.active_readers, 0)
        self.assertEqual(lock.waiting_readers, 2)

        time.sleep(1)
        # Once the writer finishes, the two readers should run parallel.
        self.assertEqual(lock.active_readers, 2)
