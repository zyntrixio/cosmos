from cosmos.campaigns.config import campaign_settings

auth_headers = {"Authorization": f"Token {campaign_settings.CAMPAIGN_API_AUTH_TOKEN}", "Bpl-User-Channel": "channel"}
