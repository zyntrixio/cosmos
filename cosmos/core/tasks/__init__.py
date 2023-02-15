import logging

import requests

from tenacity import retry
from tenacity.before import before_log
from tenacity.retry import retry_if_exception_type, retry_if_result
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed

from cosmos.core.config import core_settings
from cosmos.core.prometheus import update_metrics_exception_handler, update_metrics_hook

logger = logging.getLogger(__name__)


oauth_token_cache: dict[str, str] | None = None


@retry(
    stop=stop_after_attempt(2),
    wait=wait_fixed(1),
    reraise=True,
    before=before_log(logger, logging.INFO),
    retry_error_callback=lambda retry_state: retry_state.outcome.result() if retry_state.outcome else None,
    retry=retry_if_result(lambda resp: 501 <= resp.status_code < 600)  # noqa: PLR2004
    | retry_if_exception_type(requests.RequestException),
)
def send_request_with_metrics(
    method: str,
    url_template: str,
    url_kwargs: dict,
    *,
    exclude_from_label_url: list[str],
    headers: dict | None = None,
    data: dict | None = None,  # takes precedence over json
    json: dict | None = None,
    auth: tuple[str, str | None] | None = None,
    timeout: tuple[float, int] = (3.03, 15),
) -> requests.Response:
    """
    url_template: the url before any dynamic value is formatted into it.
    ex:
    ```python
    "{base_url}/{retailer_slug}/sample/url"
    ```

    url_kwargs: the values to be substitued into the template.
    ex:
    ```python
    {"base_url": "http://polaris-api/", "retailer_slug": "asos"}
    ```

    exclude_from_label_url: the url_kwargs' keys that we do not want to be substitued in the label url.
    ex:
    ```python
    ["retailer_slug"]
    ```

    **IMPORTANT**

    It is important that we exclude from the label url any unique field like account_holder_uuids.
    Not doing this leads to a build up of unique metrics that will lead to resource exhaustion,
    application failure, apocalypse, dragons (not the cool ones), and death.

    DO:
    ```python
    url_template="{base_url}/{account_holder_uuid}/sample/url"
    url_kwargs={"base_url": "http://polaris-api/", "account_holder_uuid": "e3ae1323-8587-4609-b32b-bd3343d42395"}
    exclude_from_label_url=["account_holder_uuid"]
    ```

    DO NOT DO:
    ```python
    url_template="{base_url}/{account_holder_uuid}/sample/url"
    url_kwargs={"base_url": "http://polaris-api/", "account_holder_uuid": "e3ae1323-8587-4609-b32b-bd3343d42395"}
    exclude_from_label_url=["base_url"] | []
    ```

    """

    label_kwargs: dict = {k: f"[{k}]" if k in exclude_from_label_url else v for k, v in url_kwargs.items()}
    label_url = url_template.format(**label_kwargs)

    hooks = {"response": update_metrics_hook(label_url)} if core_settings.ACTIVATE_TASKS_METRICS else {}

    try:
        return requests.request(
            method,
            url_template.format(**url_kwargs),
            hooks=hooks,
            headers=headers,
            data=data,
            json=json,
            auth=auth,  # type: ignore
            timeout=timeout,
        )
    except requests.HTTPError as ex:
        if core_settings.ACTIVATE_TASKS_METRICS:
            update_metrics_hook(label_url)(ex.response)
        raise

    except requests.RequestException as ex:
        if core_settings.ACTIVATE_TASKS_METRICS:
            update_metrics_exception_handler(ex, method, label_url)
        raise
