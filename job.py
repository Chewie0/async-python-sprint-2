import logging
import threading
import time
import signal
from enum import Enum
from datetime import datetime
from uuid import uuid4, UUID
from typing import Callable, Any, Optional
from utils import coroutine
from exceptions import TimeoutError

logger = logging.getLogger()


def time_break(func):
    def wrapper(*args, **kwargs):
        try:
            print("Запускаем тестируемую функцию")
            signal.alarm(10)
            res = func(*args, **kwargs)
            signal.alarm(0)
            print("Нормальное завершение")
            return res
        except Exception as e:
            print(e)
            return None


    return wrapper


class Job:
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

    def run(self) -> None:
        try:
            self._status = self.Job_stat.RUNNING
            if self._max_working_time > 0:
                self._result = self._func_with_timer()
            else:
                self._result = self._func()
            self._status = self.Job_stat.DONE
        except Exception as exc:
            self._error = exc
            self._status = self.Job_stat.FAILED

    def pause(self):
        pass

    def stop(self):
        pass

    @time_break
    def _func_with_timer(self):
        self._result = self._func()


    @coroutine
    def __start_coroutine(self) -> Any:
        result = (yield)
        yield result

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def start_at(self) -> datetime:
        return self._start_at

    @property
    def dependencies(self) -> Optional[list[UUID]]:
        return self._dependencies

    def __repr__(self) -> str:
        return f'<Job: {self._id}, {self._func.__name__}, {self._status.name}>'

    def __str__(self) -> str:
        return f'{self._func.__name__} {self._id}'
