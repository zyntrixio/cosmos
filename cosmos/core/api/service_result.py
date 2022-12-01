from typing import Any


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
        return f"<ServiceResult Exception {self.value}>"

    def __enter__(self) -> Any:
        return self.value

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        pass


def handle_service_result(result: ServiceResult) -> Any:
    if not result.success:
        raise result.value
    return result.value
