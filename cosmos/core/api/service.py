import asyncio
import logging

from collections.abc import Callable, Coroutine, Iterable
from typing import TYPE_CHECKING, Generic, TypeVar

from cosmos.core.activity.tasks import async_send_activity
from cosmos.core.api.crud import commit
from cosmos.core.error_codes import ErrorCode

if TYPE_CHECKING:

    from asyncio import Task
    from enum import Enum

    from sqlalchemy.ext.asyncio import AsyncSession

    from cosmos.db.models import Retailer

    ActivityEnumType = TypeVar("ActivityEnumType", bound="Enum")


ServiceResultValue = TypeVar("ServiceResultValue")
ServiceResultError = TypeVar("ServiceResultError", bound=Exception)


class ServiceResult(Generic[ServiceResultValue, ServiceResultError]):
    def __init__(self, value: ServiceResultValue | None = None, *, error: ServiceResultError | None = None) -> None:
        self.value = value
        self.error = error

    @property
    def success(self) -> bool:
        return self.error is None

    def __str__(self) -> str:
        return "[Success]" if self.success else f'[Exception] "{self.error}"'

    def __repr__(self) -> str:
        if self.success:
            return "<ServiceResult Success>"

        return f"<ServiceError {self.error!r}>"

    def handle_service_result(self) -> ServiceResultValue:
        if self.error:
            raise self.error

        if self.value is None:
            raise ValueError("No error or result value provided to %s", self.__class__.__name__)

        return self.value


class Service:
    def __init__(self, db_session: "AsyncSession", retailer: "Retailer") -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.db_session = db_session
        self.retailer = retailer
        self._stored_activities: list[dict] = []
        self._asyncio_tasks: set["Task"] = set()

    async def commit_db_changes(self) -> None:
        await commit(self.db_session)

    async def clear_stored_activities(self) -> None:
        self._stored_activities = []

    async def store_activity(
        self,
        activity_type: "ActivityEnumType",
        payload_formatter_fn: Callable[..., dict],
        formatter_kwargs: list[dict] | dict,
        prepend: bool = False,
    ) -> None:
        data = {
            "activity_type": activity_type,
            "payload_formatter_fn": payload_formatter_fn,
            "formatter_kwargs": formatter_kwargs,
        }
        if prepend:
            self._stored_activities.insert(0, data)
        else:
            self._stored_activities.append(data)

    async def _format_and_send_activity_in_background(
        self,
        activity_type: "ActivityEnumType",
        payload_formatter_fn: Callable[..., dict],
        formatter_kwargs: list[dict] | dict,
    ) -> None:
        async def _background_task(
            activity_type: "ActivityEnumType",
            payload_formatter_fn: Callable[..., dict],
            formatter_kwargs: list[dict] | dict,
        ) -> None:

            try:
                payload: Iterable[dict] | dict
                if isinstance(formatter_kwargs, dict):
                    payload = payload_formatter_fn(**formatter_kwargs)
                else:
                    payload = (payload_formatter_fn(**instance_kwargs) for instance_kwargs in formatter_kwargs)

                await async_send_activity(payload, routing_key=activity_type.value)

            except Exception:
                self.logger.exception(
                    "Failed to send %s activities with provided kwargs:\n%r", activity_type.name, formatter_kwargs
                )

        await self.trigger_asyncio_task(_background_task(activity_type, payload_formatter_fn, formatter_kwargs))

    async def format_and_send_stored_activities(self) -> None:
        for stored_activity in self._stored_activities:
            await self._format_and_send_activity_in_background(**stored_activity)

    async def trigger_asyncio_task(self, coro: Coroutine) -> None:
        task = asyncio.create_task(coro)
        self._asyncio_tasks.add(task)
        task.add_done_callback(self._asyncio_tasks.discard)


class ServiceError(Exception):
    def __init__(self, error_code: ErrorCode) -> None:
        self.error_code = error_code


class ServiceListError(Exception):
    def __init__(self, error_details: list[dict], status_code: int) -> None:
        self.error_details = error_details
        self.status_code = status_code
