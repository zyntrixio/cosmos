from typing import TYPE_CHECKING, Generic, TypeVar, cast

from cosmos.core.api.crud import commit
from cosmos.core.error_codes import ErrorCode

if TYPE_CHECKING:

    from sqlalchemy.ext.asyncio import AsyncSession

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

    async def commit_db_changes(self) -> None:
        await commit(self.db_session)


class ServiceError(Exception):
    def __init__(self, error_code: ErrorCode) -> None:
        self.error_code = error_code
