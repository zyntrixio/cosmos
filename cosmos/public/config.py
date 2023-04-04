from pydantic import BaseSettings, validator

from cosmos.core.config import CoreSettings, core_settings
from cosmos.core.key_vault import key_vault


class PublicSettings(BaseSettings):
    core: CoreSettings = core_settings
    PUBLIC_API_PREFIX: str = f"{core.API_PREFIX}/public"

    MAIL_EVENT_CALLBACK_USERNAME: str = "mailjet_dev"
    MAIL_EVENT_CALLBACK_PASSWORD: str = ""

    @validator("MAIL_EVENT_CALLBACK_PASSWORD")
    @classmethod
    def fetch_mail_event_callback_password(cls, v: str) -> str:
        return v or key_vault.get_secret("bpl-mail-event-callback-password")

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


public_settings = PublicSettings()
