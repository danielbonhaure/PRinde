import threading
import time
import unittest

from core.lib.sync import PrioritizedRWLock, JobsLock
from test.mock import ReaderThread, WriterThread

__author__ = 'Federico Schmidt'


class TestPrioritizedRWLock(unittest.TestCase):

    def test_readers_run_parallel(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        for i in range(0, 5):
            r = ReaderThread(lock, i, run_order)
            _threads.append(r)

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

        self.assertGreaterEqual(lock.active_readers, 2)

        for t in _threads:
            t.join()

    def test_writer_blocks_readers(self):
        lock = PrioritizedRWLock()
        _threads = []
        run_order = []

        w = WriterThread(lock, "writer", run_order)
        _threads.append(w)

        for i in range(0, 5):
            r = ReaderThread(lock, i, run_order)
            _threads.append(r)

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

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

        r1 = ReaderThread(lock, "reader 1", run_order)
        w = WriterThread(lock, "writer", run_order)
        r2 = ReaderThread(lock, "reader 2", run_order)

        _threads.append(r1)
        _threads.append(w)
        _threads.append(r2)

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

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

        r1 = ReaderThread(lock, "reader 1", run_order)
        w1 = WriterThread(lock, "writer 1", run_order, priority=0)
        r2 = ReaderThread(lock, "reader 2", run_order)
        w2 = WriterThread(lock, "writer 2", run_order, priority=1)  # This writer should run first.

        _threads.append(r1)
        _threads.append(w1)
        _threads.append(r2)
        _threads.append(w2)

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

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

        r1 = ReaderThread(lock, "reader 1", run_order)
        w1 = WriterThread(lock, "writer 1", run_order, priority=0)
        r2 = ReaderThread(lock, "reader 2", run_order)
        w2 = WriterThread(lock, "writer 2", run_order, priority=1)  # This writer should run first.

        _threads.append(r1)
        _threads.append(w1)
        _threads.append(r2)
        _threads.append(w2)

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

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

        _threads.append(ReaderThread(lock, "reader 1", run_order))
        _threads.append(WriterThread(lock, "writer 1", run_order))
        _threads.append(ReaderThread(lock, "reader 2", run_order))
        _threads.append(ReaderThread(lock, "reader 3", run_order))

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

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

    def test_jobs_lock_readers_parallelism(self):
        lock = JobsLock(max_parallel_tasks=2)
        _threads = []
        run_order = []

        _threads.append(ReaderThread(lock, "reader 1", run_order))
        _threads.append(ReaderThread(lock, "reader 2", run_order))
        _threads.append(ReaderThread(lock, "reader 3", run_order))

        for t in _threads:
            t.start()
            # Small sleep to ensure threads are executed in this order.
            time.sleep(0.05)

        self.assertEqual(lock.active_readers, 2)
        self.assertEqual(lock.waiting_readers, 1)

        time.sleep(0.5)
        # Once both readers finish, the waiting reader should be released.
        self.assertEqual(lock.active_readers, 1)
        self.assertEqual(lock.waiting_readers, 0)

        time.sleep(0.5)
        self.assertEqual(lock.active_readers, 0)
        self.assertEqual(lock.waiting_readers, 0)
