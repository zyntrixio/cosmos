from datetime import UTC, datetime

import requests

from urllib3 import Retry

from cosmos.core.config import core_settings

from . import logger, oauth_token_cache  # noqa: F401


def retry_session() -> requests.Session:  # pragma: no cover
    # deepcode ignore MissingClose: snyk wrongly assume this as a database Session and requires .close()
    session = requests.Session()
    retry = Retry(total=3, allowed_methods=None, status_forcelist=[501, 502, 503, 504], backoff_factor=1.0)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_new_token() -> dict[str, str]:
    try:
        resp = retry_session().get(
            f"{core_settings.AZURE_OAUTH2_TOKEN_URL}/metadata/identity/oauth2/token",
            params={
                "api-version": "2019-06-04",
                "resource": core_settings.CALLBACK_OAUTH2_RESOURCE,
            },
            headers={"Metadata": "true"},
        )
        resp.raise_for_status()

    except requests.RequestException as ex:  # pragma: no cover
        logger.error("failed to fetch callback oauth2 token from azure.")
        raise ex

    return resp.json()


def _stored_token_is_valid(stored_token: dict[str, str]) -> bool:
    try:
        if (
            float(stored_token["not_before"])
            <= datetime.now(tz=UTC).timestamp()
            <= float(stored_token["expires_on"]) - 300
        ):
            return True

    except (ValueError, KeyError) as ex:  # pragma: no cover
        logger.exception("invalid callback oauth2 token stored.", exc_info=ex)

    return False


def get_callback_oauth_header() -> dict:
    """
    :return: access token type, access token value
    """
    global oauth_token_cache

    if oauth_token_cache is None or not _stored_token_is_valid(oauth_token_cache):
        oauth_token_cache = _get_new_token()

    return {"Authorization": f"{oauth_token_cache['token_type']} {oauth_token_cache['access_token']}"}


if __name__ == "__main__":  # pragma: no cover
    pass
