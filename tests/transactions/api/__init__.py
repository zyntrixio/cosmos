from cosmos.transactions.config import tx_settings

auth_headers = {"Authorization": f"Token {tx_settings.VELA_API_AUTH_TOKEN}", "Bpl-User-Channel": "channel"}
