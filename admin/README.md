# Admin panel

BPL Admin Dashboard

## Configuration

- create a `.env` file in the root directory
- add your configurations based on the environmental variables required in `settings.py`
- additionally, the following environment variables need to be set in order for the Azure Active Directory OIDC authentication flow to work

  - EVENT_HORIZON_CLIENT_KEY
  - EVENT_HORIZON_CLIENT_SECRET

  > **NOTE:**  The EVENT_HORIZON_CLIENT_KEY can be obtained from the relevant [Azure App Registration](https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationsListBlade) (look for "Event Horizon (local/dev)"). The EVENT_HORIZON_CLIENT_SECRET can then be obtained from the "_Secrets - Development_" 1Password vault.

## Running
From cosmos project root

- `poetry install -E admin`
- `poetry run python admin/wsgi.py`

## Testing

- `poetry run pytest tests`
