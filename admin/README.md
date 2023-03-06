# Admin panel

BPL Admin Dashboard

## Configuration

- create a `.env` file in the root directory
- add your configurations based on the environmental variables required in `settings.py`
- additionally, the following environment variables need to be set in order for the Azure Active Directory OIDC authentication flow to work

  - COSMOS_ADMIN_CLIENT_ID
  - COSMOS_ADMIN_CLIENT_SECRET

  > **NOTE:**  The COSMOS_ADMIN_CLIENT_ID can be obtained from the relevant [Azure App Registration](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade) (look for "Cosmos Admin (local/dev)"). The COSMOS_ADMIN_CLIENT_SECRET can then be obtained from the "_Secrets - Development_" 1Password vault.

## Running
From cosmos project root

- `poetry install --all-extras`
- `poetry run python admin/wsgi.py`

## Testing

- `poetry run pytest tests`
