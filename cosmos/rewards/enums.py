from enum import Enum


class RewardTypeStatuses(str, Enum):
    ACTIVE = "active"
    CANCELLED = "cancelled"
    ENDED = "ended"
    DELETED = "deleted"


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
