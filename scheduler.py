import logging
import time
import threading
from datetime import datetime

from job import Job
from utils import coroutine, Thread_kill

logger = logging.getLogger()


class Scheduler(threading.Thread):
    def __init__(self, pool_size: int = 10, sleep_interval: int = 2):
        threading.Thread.__init__(self, target=self.run)
        self._pool_size = pool_size
        self._is_run: bool = True
        self._pending_jobs = []
        self._running_jobs = []
        self._done_jobs = []
        self._failed_jobs = []
        self._exec = self._executor()
        self._kill = threading.Event()
        self._interval = sleep_interval

    def schedule(self, job: Job) -> None:
        if len(self._pending_jobs) + len(self._running_jobs) <= self._pool_size:
            self._pending_jobs.append(job)

    def run(self) -> None:
        self._kill.set()
        self._is_run = True
        exc = self._exec
        while self._is_run:
            self._kill.wait()
            logger.info('MAKING SHEDULE')
            if self._kill.is_set():
                logger.info('STOPPING SHEDULE %s', len(self._pending_jobs))
            logger.info('Count of pending tasks %s', len(self._pending_jobs))
            if len(self._pending_jobs) > 0:
                job = self._pending_jobs.pop(0)
                job.get_job_failed(self.job_failed)
                job.get_completed_job(self.complete_job)
                if self.check_deps_for_job(job):
                    logger.info('Send to executor job %s', job)
                    exc.send(job)
                else:
                    self._pending_jobs.append(job)
            time.sleep(1)

    @coroutine
    def _executor(self) -> None:
        while True:
            job = yield
            if job.start_at > datetime.now():
                thread = threading.Timer(interval=job.start_at.timestamp() - time.time(), function=job.run)
            elif job.max_working_time > 0:
                thread = Thread_kill(target=job.run)
                job.get_thread(thread)
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

    @property
    def done_jobs(self) -> list:
        return self._done_jobs

    @property
    def pending_jobs(self) -> list:
        return self._pending_jobs

    def job_failed(self, job: Job) -> None:
        if job.tries > 0:
            logger.info('We have to try %s times', job.tries)
            time.sleep(0.5)
            job.restart()
            self.schedule(job)
            return
        self._running_jobs.remove(job)
        self._failed_jobs.append(job)

    def complete_job(self, job: Job) -> None:
        self._running_jobs.remove(job)
        self._done_jobs.append(job)

    def check_deps_for_job(self, job: Job) -> bool:
        if job.dependencies is not None:
            for dep in job.dependencies:
                if dep not in [item.id for item in self._done_jobs]:
                    logger.error('Dependencies not complete %s', job)
                    return False
        return True

    def restart(self) -> None:
        logger.info('RESTART SHEDULE %s', len(self._pending_jobs))
        self._kill.set()

    def stop(self) -> None:
        logger.info('STOP SHEDULE')
        self._kill.clear()

    def join(self, timeout: int = None) -> None:
        self._is_run = False
        for j in self._running_jobs:
            j.stop()
        logger.info('END SHEDULE')
        self._kill.set()
        threading.Thread.join(self)
