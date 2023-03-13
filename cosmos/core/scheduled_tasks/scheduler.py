import logging

from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from logging import Logger
from typing import Any, Protocol
from uuid import uuid4

from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import undefined

from cosmos.core.config import core_settings, redis
from cosmos.core.scheduled_tasks import logger as scheduled_tasks_logger

from . import logger

LOCK_TIMEOUT_SECS = 3600


class Runner(Protocol):
    uid: str
    name: str


def acquire_lock(runner: Runner) -> Callable:
    """
    Decorator for use with scheduled tasks to ensure a scheduled task won't be
    run concurrently somewhere else.
    """

    def decorater(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> None:  # noqa ANN401
            func_lock_key = f"{core_settings.REDIS_KEY_PREFIX}{runner.name}:{func.__qualname__}"
            value = f"{runner.uid}:{datetime.now(tz=UTC)}"
            if redis.set(func_lock_key, value, LOCK_TIMEOUT_SECS, nx=True):
                # This assumes jobs will be completed within LOCK_TIMEOUT_SECS
                # seconds and if the lock expires then another process can run
                # the function without consequence
                try:
                    func(*args, **kwargs)
                except Exception as ex:
                    logger.exception("Unexpected error occurred while running '%s'", func.__qualname__, exc_info=ex)
                finally:
                    redis.delete(func_lock_key)
            else:
                msg = f"{runner} could not run {func.__qualname__}. Could not acquire the lock."
                lock_val = redis.get(func_lock_key)
                if lock_val is not None:
                    try:
                        runner_uid, timestamp = lock_val.split(":", 1)
                        msg += f"\nProcess locked since: {timestamp} by runner of id: {runner_uid}"
                    except ValueError:
                        logger.error(f"unexpected lock value ({lock_val})")

                logger.info(msg)

        return wrapper

    return decorater


class CronScheduler:  # pragma: no cover
    name = "cron-scheduler"
    default_schedule = "* * * * *"
    tz = "Europe/London"

    def __init__(self, *, log: Logger | None = None) -> None:
        self.uid = str(uuid4())
        self.log = log if log is not None else logging.getLogger("cron-scheduler")
        self._scheduler = BlockingScheduler()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(id: {self.uid})"

    def _get_trigger(self, schedule: Callable) -> CronTrigger:
        try:
            return CronTrigger.from_crontab(schedule, timezone=self.tz)
        except ValueError:
            self.log.error(
                f"Schedule '{schedule}' is not in a recognised format! "
                f"Reverting to default of '{self.default_schedule}'."
            )
            return CronTrigger.from_crontab(self.default_schedule, timezone=self.tz)

    def add_job(
        self,
        job_func: Callable,
        *,
        schedule_fn: Callable,
        coalesce_jobs: bool | None = None,
        args: list | tuple | None = None,
        kwargs: dict | None = None,
    ) -> None:
        if coalesce_jobs is None:
            coalesce_jobs = undefined

        schedule = schedule_fn()
        if not schedule:
            self.log.warning(f"No schedule provided! Reverting to default of '{self.default_schedule}'.")
            schedule = self.default_schedule

        self._scheduler.add_job(
            job_func, trigger=self._get_trigger(schedule), coalesce=coalesce_jobs, args=args, kwargs=kwargs
        )

    def run(self) -> None:
        self._scheduler.start()


cron_scheduler = CronScheduler(log=scheduled_tasks_logger)
