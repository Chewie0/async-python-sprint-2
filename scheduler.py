import os
import pickle
import time

from utils import coroutine, SingletonMeta
import threading
from datetime import datetime


class Scheduler(threading.Thread):
    def __init__(self, pool_size: int = 10):
        threading.Thread.__init__(self, target=self.run)
        self._pool_size = pool_size
        self._is_run: bool = True
        self._pending_jobs = []
        self._running_jobs = []
        self._done_jobs = []



    def schedule(self, task):
        if len(self._pending_jobs) + len(self._running_jobs) <= self._pool_size:
            self._pending_jobs.append(task)

    def run(self):
        exc = self._executor()
        while self._is_run:
            print(self.is_running)
            if len(self._pending_jobs) > 0:
                exc.send(self._pending_jobs.pop(0))
            time.sleep(1)


    @coroutine
    def _executor(self):
        while True:
            job = yield
            if job.start_at > datetime.now():
                thread = threading.Timer(interval=job.start_at.timestamp() - time.time(), function=job.run)
            else:
                thread = threading.Thread(target=job.run)
            self._running_jobs.append(job)
            thread.start()



    def restart(self):
        pass

    @property
    def is_running(self) -> bool:
        return self._is_run

    def join(self, timeout=None):
        self._is_run = False
        threading.Thread.join(self)


    def stop(self):
        self._is_run = False
        with open('scheduler.pickle', 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls):
        with open('scheduler.pickle', 'rb') as f:
            scheduler = pickle.load(f)
        os.remove('scheduler.pickle')
        scheduler._executor = scheduler._executor()
        scheduler._is_active = True
        return scheduler