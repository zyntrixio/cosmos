from pydantic import BaseSettings

from cosmos.core.config import CoreSettings, core_settings


class PublicSettings(BaseSettings):
    core: CoreSettings = core_settings
    PUBLIC_API_PREFIX: str = f"{core.API_PREFIX}/public"

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


public_settings = PublicSettings()
