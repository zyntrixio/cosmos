from pydantic import BaseSettings, validator

from cosmos.core.config import CoreSettings, core_settings
from cosmos.core.key_vault import key_vault


class AccountSettings(BaseSettings):
    core: CoreSettings = core_settings
    ACCOUNT_API_PREFIX: str = f"{core.API_PREFIX}/loyalty"

    ACCOUNT_API_AUTH_TOKEN: str = ""

    @validator("ACCOUNT_API_AUTH_TOKEN")
    @classmethod
    def fetch_accounts_api_auth_token(cls, v: str | None) -> str:
        return v or key_vault.get_secret("bpl-accounts-api-auth-token")

    ACCOUNT_HOLDER_ACTIVATION_TASK_NAME: str = "account-holder-activation"
    ENROLMENT_CALLBACK_TASK_NAME: str = "enrolment-callback"

    CREATE_CAMPAIGN_BALANCES_TASK_NAME: str = "create-campaign-balances"
    DELETE_CAMPAIGN_BALANCES_TASK_NAME: str = "delete-campaign-balances"
    PENDING_ACCOUNTS_ACTIVATION_TASK_NAME: str = "pending-accounts-activation"
    CANCEL_REWARDS_TASK_NAME: str = "cancel-rewards"
    PROCESS_PENDING_REWARD_TASK_NAME: str = "pending-reward-allocation"
    CONVERT_PENDING_REWARDS_TASK_NAME: str = "convert-pending-rewards"
    DELETE_PENDING_REWARDS_TASK_NAME: str = "delete-pending-rewards"
    ANONYMISE_ACCOUNT_HOLDER_TASK_NAME: str = "anonymise-account-holder"
    RESET_BALANCES_SCHEDULE: str = "0 3 * * *"
    RESET_BALANCE_NUDGES_SCHEDULE = "0 3 * * *"
    USE_CALLBACK_OAUTH2: bool = True

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


account_settings = AccountSettings()
