from urllib.parse import urlparse

from pydantic import BaseSettings, validator

from cosmos.core.config import CoreSettings, core_settings
from cosmos.core.key_vault import key_vault


class CampaignSettings(BaseSettings):
    core: CoreSettings = core_settings
    CAMPAIGN_API_PREFIX: str = f"{core.API_PREFIX}/campaigns"

    REQUEST_TIMEOUT: int = 2
    CAMPAIGN_HOST_URL: str = "http://cosmos-campaigns-api"
    CAMPAIGN_BASE_URL: str = ""

    @validator("CAMPAIGN_BASE_URL", pre=False, always=True)
    @classmethod
    def assemble_url(cls, v: str | None, values: dict) -> str:
        return v or urlparse(values["CAMPAIGN_HOST_URL"])._replace(path=values["CAMPAIGN_API_PREFIX"]).geturl()

    CAMPAIGN_API_AUTH_TOKEN: str = ""

    @validator("CAMPAIGN_API_AUTH_TOKEN", pre=False, always=True)
    @classmethod
    def fetch_campaigns_api_auth_token(cls, v: str | None) -> str:
        return v or key_vault.get_secret("bpl-campaigns-api-auth-token")

    class Config:
        case_sensitive = True
        env_file = "local.env"
        env_file_encoding = "utf-8"


campaign_settings = CampaignSettings()
