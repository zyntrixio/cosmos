import logging

from pydantic import AnyHttpUrl, BaseSettings, validator

from cosmos.core.config import CoreSettings, core_settings
from cosmos.core.key_vault import key_vault


class RewardSettings(BaseSettings):
    core: CoreSettings = core_settings

    BLOB_STORAGE_DSN: str = ""
    BLOB_IMPORT_CONTAINER = "carina-imports"
    BLOB_ARCHIVE_CONTAINER = "carina-archive"
    BLOB_ERROR_CONTAINER = "carina-errors"
    BLOB_CLIENT_LEASE_SECONDS = 60
    BLOB_IMPORT_LOGGING_LEVEL = logging.WARNING
    BLOB_IMPORT_SCHEDULE = "*/5 * * * *"

    PRE_LOADED_REWARD_BASE_URL: AnyHttpUrl
    MESSAGE_IF_NO_PRE_LOADED_REWARDS: bool = False

    JIGSAW_AGENT_USERNAME: str = "Bink_dev"
    JIGSAW_AGENT_PASSWORD: str = None  # type: ignore [assignment]

    @validator("JIGSAW_AGENT_PASSWORD", pre=True, always=True)
    @classmethod
    def fetch_jigsaw_agent_password(cls, v: str | None) -> str:
        return v or key_vault.get_secret("bpl-rewards-agent-jigsaw-password")

    JIGSAW_AGENT_ENCRYPTION_KEY: str = None  # type: ignore [assignment]

    @validator("JIGSAW_AGENT_ENCRYPTION_KEY", pre=True, always=True)
    @classmethod
    def fetch_jigsaw_agent_encryption_key(cls, v: str | None) -> str:
        return v or key_vault.get_secret("bpl-rewards-agent-jigsaw-encryption-key")

    REWARD_ISSUANCE_TASK_NAME = "reward-issuance"
    REWARD_ISSUANCE_REQUEUE_BACKOFF_SECONDS: int = 60 * 60 * 12  # 12 hours

    PENDING_REWARDS_SCHEDULE: str = "0 2 * * *"

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


reward_settings = RewardSettings()
