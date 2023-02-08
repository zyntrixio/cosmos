from abc import ABC, abstractmethod
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from httpretty.core import HTTPrettyRequest


class AnswerBotBase(ABC):
    def __init__(self) -> None:
        self.calls: dict[str, int] = defaultdict(int)

    def _update_calls_and_get_endpoint(self, uri: str) -> str:
        _, endpoint_name = uri.rsplit("/", 1)
        self.calls[endpoint_name] += 1
        return endpoint_name

    @abstractmethod
    def response_generator(
        self, request: "HTTPrettyRequest", uri: str, response_headers: dict
    ) -> tuple[int, dict, str]:
        ...
