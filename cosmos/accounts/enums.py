from datetime import date, datetime
from enum import Enum
from typing import Any


class AccountHolderStatuses(Enum):
    PENDING = "pending"
    ACTIVE = "active"
    INACTIVE = "inactive"
    FAILED = "failed"


class RewardApiStatuses(Enum):
    ISSUED = "issued"
    CANCELLED = "cancelled"
    REDEEMED = "redeemed"
    EXPIRED = "expired"


class MarketingPreferenceValueTypes(Enum):
    BOOLEAN = bool
    INTEGER = int
    FLOAT = float
    STRING = str
    STRING_LIST = list
    DATE = date
    DATETIME = datetime

    def convert_value(self, v: str) -> Any:  # noqa: ANN401
        if self.value == bool:
            return v.lower() in ("true", "1", "t", "yes", "y")

        if self.value == list:
            return v.split(", ")

        if self.value in (date, datetime):
            return self.value.fromisoformat(v)

        return self.value(v)
