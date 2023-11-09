import logging
from enum import Enum
from datetime import datetime, timedelta
from threading import Timer
from uuid import uuid4, UUID
from typing import Callable, Any, Optional


logger = logging.getLogger()


class Job():
    class Job_stat(Enum):
        PENDING = 1
        RUNNING = 2
        DONE = 3
        FAILED = 4

    def __init__(self,
                 func: Callable[[], Any],
                 start_at: datetime = datetime.now(),
                 max_working_time: int = 0,
                 tries: int = 0,
                 dependencies: Optional[list[UUID]] = None,
                 ):
        self._start_at = start_at
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies
        self._id = uuid4()
        self._func = func
        self._result = None
        self._error = None
        self._thread_obj = None
        self._status = self.Job_stat.PENDING
        self._complite_job = None
        self._job_failed = None
        self._thread = None

    def run(self) -> None:
        try:
            self._status = self.Job_stat.RUNNING
            if self._max_working_time > 0:
                logger.info('Job run with timeout %s', self)
                self._result = self._func_with_timer()
            else:
                logger.info('Job run %s', self)
                self._result = self._func()
            self._status = self.Job_stat.DONE
            self._complite_job(self)
            logger.info('Job complete %s', self)
        except Exception as exc:
            self.failed(exc)

    def get_complited_job(self, complite_job) -> None:
        self._complite_job = complite_job

    def get_job_failed(self, job_failed) -> None:
        self._job_failed = job_failed

    def get_thread(self, thread):
        self._thread = thread

    def terminate_job(self) -> None:
        if self._status == self.Job_stat.RUNNING:
            logger.info('Killed thread of job %s ', self)
            self._thread.kill()
            self._status = self.Job_stat.FAILED
            self._result = None
            self._error = None

    def stop(self) -> None:
        self._status = self.Job_stat.PENDING
        self._error = None

    def restart(self) -> None:
        self._tries -= 1
        self._start_at = datetime.now() + timedelta(seconds=1)
        self._status = self.Job_stat.PENDING
        self._result = None
        self._error = None

    def failed(self, error: Exception) -> None:
        logger.error('Job %s failed', self)
        self._job_failed(self)
        self._error = error
        self._status = self.Job_stat.FAILED

    def _func_with_timer(self) -> None:
        self._timer = Timer(self.max_working_time, self.terminate_job)
        self._timer.start()
        self._result = self._func()

    @property
    def status(self) -> Job_stat:
        return self._status

    @property
    def result(self) -> Any:
        return self._result

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def start_at(self) -> datetime:
        return self._start_at

    @property
    def tries(self) -> int:
        return self._tries

    @property
    def max_working_time(self) -> int:
        return self._max_working_time

    @property
    def dependencies(self) -> Optional[list[UUID]]:
        return self._dependencies

    def __repr__(self) -> str:
        return f'<Job: {self._id}, {self._func.__name__}, {self._status.name}>'

    def __str__(self) -> str:
        return f'{self._func.__name__} {self._id}'
