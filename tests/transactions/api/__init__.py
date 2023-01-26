from cosmos.core.config import settings

auth_headers = {"Authorization": f"Token {settings.VELA_API_AUTH_TOKEN}", "Bpl-User-Channel": "channel"}
