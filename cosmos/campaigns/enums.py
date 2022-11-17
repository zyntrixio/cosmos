from enum import Enum, IntEnum, auto

from fastapi import HTTPException, status


class CampaignStatuses(Enum):
    ACTIVE = "active"
    DRAFT = "draft"
    CANCELLED = "cancelled"
    ENDED = "ended"

    @classmethod
    def status_transitions(cls) -> dict[Enum, tuple]:
        return {
            cls.ACTIVE: (cls.CANCELLED, cls.ENDED),
            cls.DRAFT: (cls.ACTIVE,),
            cls.CANCELLED: (),
            cls.ENDED: (),
        }

    def is_valid_status_transition(self, current_status: Enum) -> bool:
        return self in self.status_transitions()[current_status]


class LoyaltyTypes(Enum):
    ACCUMULATOR = "accumulator"
    STAMPS = "stamps"


class TransactionProcessingStatuses(Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    NO_ACTIVE_CAMPAIGNS = "no-active-campaigns"


class HttpErrors(Enum):
    NO_ACTIVE_CAMPAIGNS = HTTPException(
        detail={"display_message": "No active campaigns found for retailer.", "code": "NO_ACTIVE_CAMPAIGNS"},
        status_code=status.HTTP_404_NOT_FOUND,
    )
    INVALID_RETAILER = HTTPException(
        detail={
            "display_message": "Requested retailer is invalid.",
            "code": "INVALID_RETAILER",
        },
        status_code=status.HTTP_403_FORBIDDEN,
    )
    INVALID_TOKEN = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "display_message": "Supplied token is invalid.",
            "code": "INVALID_TOKEN",
        },
    )
    DUPLICATE_TRANSACTION = HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"display_message": "Duplicate Transaction.", "code": "DUPLICATE_TRANSACTION"},
    )
    USER_NOT_FOUND = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"display_message": "Unknown User.", "code": "USER_NOT_FOUND"},
    )
    USER_NOT_ACTIVE = HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={"display_message": "User Account not Active", "code": "USER_NOT_ACTIVE"},
    )
    GENERIC_HANDLED_ERROR = HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail={
            "display_message": "An unexpected system error occurred, please try again later.",
            "code": "INTERNAL_ERROR",
        },
    )
    INVALID_STATUS_REQUESTED = HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "display_message": "The requested status change could not be performed.",
            "code": "INVALID_STATUS_REQUESTED",
        },
    )
    NO_CAMPAIGN_FOUND = HTTPException(
        detail={
            "display_message": "Campaign not found for provided slug.",
            "code": "NO_CAMPAIGN_FOUND",
        },
        status_code=status.HTTP_404_NOT_FOUND,
    )
    DELETE_FAILED = HTTPException(
        detail={
            "display_message": "The campaign could not be deleted.",
            "code": "DELETE_FAILED",
        },
        status_code=status.HTTP_409_CONFLICT,
    )


class HttpsErrorTemplates(Enum):
    INVALID_STATUS_REQUESTED = {
        "display_message": "The requested status change could not be performed.",
        "code": "INVALID_STATUS_REQUESTED",
    }

    NO_CAMPAIGN_FOUND = {
        "display_message": "Campaign not found for provided slug.",
        "code": "NO_CAMPAIGN_FOUND",
    }

    MISSING_CAMPAIGN_COMPONENTS = {
        "display_message": "the provided campaign(s) could not be made active",
        "code": "MISSING_CAMPAIGN_COMPONENTS",
    }

    def value_with_slugs(self, campaign_slugs: list[str]) -> dict:
        self.value["campaigns"] = campaign_slugs  # type: ignore [assignment]
        return self.value


class RewardAdjustmentStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    SUCCESS = "success"
    ACCOUNT_HOLDER_DELETED = "account_holder_deleted"


class RewardCap(IntEnum):
    ONE = auto()
    TWO = auto()
    THREE = auto()
    FOUR = auto()
    FIVE = auto()
    SIX = auto()
    SEVEN = auto()
    EIGHT = auto()
    NINE = auto()
    TEN = auto()
