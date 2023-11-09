import json
import logging
import os
import pickle
import time
from utils import coroutine
import threading
from datetime import datetime

logger = logging.getLogger()


class Scheduler(threading.Thread):
    def __init__(self, pool_size: int = 10, sleep_interval: int=2):
        threading.Thread.__init__(self,target=self.run)
        self._pool_size = pool_size
        self._is_run: bool = True
        self._pending_jobs = []
        self._running_jobs = []
        self._done_jobs = []
        self._failed_jobs = []
        self._exec = self._executor()
        self._kill = threading.Event()
        self._interval = sleep_interval

    def schedule(self, job) -> None:
        if len(self._pending_jobs) + len(self._running_jobs) <= self._pool_size:
            self._pending_jobs.append(job)

    '''
    def start(self) -> None:
        logger.info('Start shedule')
        self._is_run = True
        self.loop_thread = threading.Thread(target=self.run)
        self.loop_thread.start()
    '''

    def run(self) -> None:
        logger.info('Start shedule')
        self._is_run = True
        exc = self._exec
        while self._is_run:
            logger.info('Count of pending tasks %s', len(self._pending_jobs))
            if len(self._pending_jobs) > 0:
                job = self._pending_jobs.pop(0)
                if self.check_deps_for_job(job):
                    logger.info('Send to run job %s', job)
                    exc.send(job)
                else:
                    self._pending_jobs.append(job)
            time.sleep(1)
            is_killed = self._kill.wait(self._interval)
            if is_killed:
                break

    @coroutine
    def _executor(self) -> None:
        while True:
            job = yield
            if job.start_at > datetime.now():
                thread = threading.Timer(interval=job.start_at.timestamp() - time.time(), function=job.run)
            else:
                thread = threading.Thread(target=job.run)
            self._running_jobs.append(job)
            thread.start()

    @property
    def is_running(self) -> bool:
        return self._is_run

    @property
    def running_jobs(self) -> list:
        return self._running_jobs

    def job_failed(self, job) -> None:
        if job.tries > 0:
            logger.info('We have to try %s times', job.tries)
            time.sleep(3)
            job.restart()
            self.schedule(job)
            return
        self._running_jobs.remove(job)
        self._failed_jobs.append(job)

    def complete_job(self, job) -> None:
        self._running_jobs.remove(job)
        self._done_jobs.append(job)

    def check_deps_for_job(self, job) -> bool:
        if job.dependencies is not None:
            for dep in job.dependencies:
                if dep not in [item.id for item in self._done_jobs]:
                    logger.error('Dependencies not complete')
                    return False
        return True

    def restart(self) -> None:
        self._kill.clear()

    def stop(self) -> None:
        logger.info('Stop shedule')
        #self._is_run = False
        for j in self._running_jobs:
            j.stop()
        self._kill.set()

    def join(self, timeout=None) -> None:
        self._is_run = False
        threading.Thread.join(self)

