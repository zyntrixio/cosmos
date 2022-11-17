import logging

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logger = logging.getLogger("key_vault")


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

    def get_secret(self, secret_name: str) -> str | None:
        if not self.client:
            return "testing-token"

        try:
            return self.client.get_secret(secret_name).value
        except (ServiceRequestError, ResourceNotFoundError, HttpResponseError) as ex:
            raise KeyVaultError(f"Could not retrieve secret {secret_name}") from ex
