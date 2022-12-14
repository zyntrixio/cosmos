from typing import TYPE_CHECKING, Any

from cosmos.core.error_codes import ErrorCode

if TYPE_CHECKING:

    from sqlalchemy.ext.asyncio import AsyncSession

    from cosmos.db.models import Retailer


class ServiceResult:
    def __init__(self, val: Any) -> None:
        self.value = val

    @property
    def success(self) -> bool:
        return not isinstance(self.value, Exception)

    def __str__(self) -> str:
        if self.success:
            return "[Success]"
        return f'[Exception] "{self.value}"'

    def __repr__(self) -> str:
        if not self.success:
            return "<ServiceResult Success>"
        return f"<ServiceException {self.value}>"

    def __enter__(self) -> Any:
        return self.value

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        pass


class Service:
    def __init__(self, db_session: "AsyncSession", retailer: "Retailer") -> None:
        self.db_session = db_session
        self.retailer = retailer


class ServiceException(Exception):
    def __init__(self, error_code: ErrorCode) -> None:
        self.error_code = error_code


def handle_service_result(result: ServiceResult) -> Any:
    if not result.success:
        raise result.value
    return result.value
