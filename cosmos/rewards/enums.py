from enum import Enum


class RewardUpdateStatuses(Enum):
    CANCELLED = "cancelled"
    REDEEMED = "redeemed"


class RewardFetchType(Enum):
    PRE_LOADED = "pre_loaded"


class FileAgentType(Enum):
    IMPORT = "import"
    UPDATE = "update"


class PendingRewardActions(Enum):
    REMOVE = "remove"
    CONVERT = "convert"


class PendingRewardMigrationActions(Enum):
    REMOVE = "remove"
    CONVERT = "convert"
    TRANSFER = "transfer"

    @classmethod
    def get_choices(cls, allow_transfer: bool) -> list[str]:
        return [attr.value for attr in cls if attr != cls.TRANSFER or allow_transfer]
