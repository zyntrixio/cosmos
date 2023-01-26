from typing import TYPE_CHECKING, Callable, Generic, TypeVar, cast

from cosmos.core.activity.utils import format_and_send_activity_in_background
from cosmos.core.api.crud import commit
from cosmos.core.error_codes import ErrorCode

if TYPE_CHECKING:

    from sqlalchemy.ext.asyncio import AsyncSession

    from cosmos.core.activity.utils import ActivityEnumType
    from cosmos.db.models import Retailer


ServiceResultValue = TypeVar("ServiceResultValue")
ServiceResultError = TypeVar("ServiceResultError", bound=Exception)


class ServiceResult(Generic[ServiceResultValue, ServiceResultError]):
    def __init__(self, value: ServiceResultValue = None, *, error: ServiceResultError | None = None) -> None:
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

        return cast(ServiceResultValue, self.value)


class Service:
    def __init__(self, db_session: "AsyncSession", retailer: "Retailer") -> None:
        self.db_session = db_session
        self.retailer = retailer
        self._stored_activities: list[dict] = []

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

    async def format_and_send_stored_activities(self) -> None:
        for stored_activity in self._stored_activities:
            await format_and_send_activity_in_background(**stored_activity)


class ServiceError(Exception):
    def __init__(self, error_code: ErrorCode) -> None:
        self.error_code = error_code


class ServiceListError(Exception):
    def __init__(self, error_details: list[dict], status_code: int) -> None:
        self.error_details = error_details
        self.status_code = status_code
