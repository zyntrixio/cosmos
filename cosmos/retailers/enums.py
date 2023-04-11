import enum


class EmailTypeSlugs(enum.Enum):
    WELCOME_EMAIL = enum.auto()
    REWARD_ISSUANCE = enum.auto()
    BALANCE_RESET = enum.auto()
    PURCHASE_PROMPT = enum.auto()


class EmailTemplateKeys(enum.Enum):
    EMAIL = "email"
    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    ACCOUNT_NUMBER = "account_number"
    MARKETING_OPT_OUT_LINK = "marketing_opt_out_link"
    REWARD_URL = "reward_url"


class RetailerStatuses(enum.Enum):
    TEST = enum.auto()
    ACTIVE = enum.auto()
    INACTIVE = enum.auto()
    DELETED = enum.auto()
    ARCHIVED = enum.auto()
