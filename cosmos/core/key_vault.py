import logging
import sys

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pydantic import BaseSettings, validator

logger = logging.getLogger("key_vault")


class VaultSettings(BaseSettings):
    KEY_VAULT_URI: str = "https://bink-uksouth-dev-com.vault.azure.net/"
    TESTING: bool = False

    @validator("TESTING")
    @classmethod
    def is_test(cls, v: bool) -> bool:
        command = sys.argv[0]
        args = sys.argv[1:] if len(sys.argv) > 1 else []

        return True if "pytest" in command or any("test" in arg for arg in args) else v  # noqa: PLR2004

    MIGRATING: bool = False

    @validator("MIGRATING")
    @classmethod
    def is_migration(cls, v: bool) -> bool:
        command = sys.argv[0]
        return True if "alembic" in command else v  # noqa: PLR2004

    class Config:
        case_sensitive = True
        # env var settings priority ie priority 1 will override priority 2:
        # 1 - env vars already loaded (ie the one passed in by kubernetes)
        # 2 - env vars read from *local.env file
        # 3 - values assigned directly in the Settings class
        env_file = "local.env"
        env_file_encoding = "utf-8"


vault_settings = VaultSettings()


class KeyVaultError(Exception):
    pass


class KeyVault:
    def __init__(self, vault_url: str, test_or_migration: bool = False) -> None:
        if test_or_migration:
            self.client = None
            logger.info("Key Vault not initialised as this is either a test or a migration.")
        else:
            self.client = SecretClient(
                vault_url=vault_url,
                credential=DefaultAzureCredential(
                    additionally_allowed_tenants=["a6e2367a-92ea-4e5a-b565-723830bcc095"]
                ),
            )

    def get_secret(self, secret_name: str) -> str:
        if not self.client:
            return "testing-token"

        try:
            value = self.client.get_secret(secret_name).value
        except (ServiceRequestError, ResourceNotFoundError, HttpResponseError) as ex:
            raise KeyVaultError(f"Could not retrieve secret {secret_name}") from ex

        if not value:
            raise KeyVaultError(f"Secret {secret_name} returned a None value.")

        return value


key_vault = KeyVault(vault_settings.KEY_VAULT_URI, vault_settings.TESTING or vault_settings.MIGRATING)
