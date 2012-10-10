""" Extend Queue class for python 2.4"""
import threading
import sys
from Queue import Queue

if sys.version >= "2.5":
    class PumpQueue(Queue):
        def __init__(self, maxsize=0):
            Queue.__init__(self, maxsize)
else:
    class PumpQueue(Queue):
        def __init__(self, maxsize=0):
            Queue.__init__(self, maxsize)
            self.all_tasks_done = threading.Condition(self.mutex)
            self.unfinished_tasks = 0

        def _put(self, item):
            Queue._put(self, item)
            self.unfinished_tasks += 1

        def task_done(self):
            self.all_tasks_done.acquire()
            try:
                unfinished = self.unfinished_tasks - 1
                if unfinished <= 0:
                    if unfinished < 0:
                        raise ValueError('task_done() called too many times')
                    self.all_tasks_done.notifyAll()
                self.unfinished_tasks = unfinished
            finally:
                self.all_tasks_done.release()

        def join(self):
            self.all_tasks_done.acquire()
            try:
                while self.unfinished_tasks:
                    self.all_tasks_done.wait()
            finally:
                self.all_tasks_done.release()