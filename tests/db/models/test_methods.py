from typing import TYPE_CHECKING

import pytest

from cosmos.public.config import public_settings

if TYPE_CHECKING:
    from collections.abc import Generator

    from cosmos.public.config import PublicSettings
    from tests.conftest import SetupType


@pytest.fixture(scope="function")
def overridable_public_settings() -> "Generator[PublicSettings, None, None]":
    public_url_origin = public_settings.core.PUBLIC_URL
    public_api_prefix_origin = public_settings.PUBLIC_API_PREFIX

    yield public_settings

    public_settings.core.PUBLIC_URL = public_url_origin
    public_settings.PUBLIC_API_PREFIX = public_api_prefix_origin


def test_account_holder_marketing_opt_out_link(
    setup: "SetupType", overridable_public_settings: "PublicSettings"
) -> None:
    _, retailer, account_holder = setup

    expected_url = (
        f"http://test.url/relative/path/{retailer.slug}/marketing/unsubscribe?u={account_holder.opt_out_token}"
    )

    for public_url, public_api_prefix in (
        ("http://test.url", "/relative/path"),
        ("http://test.url/", "/relative/path"),
        ("http://test.url/relative", "/path"),
        ("http://test.url/", "/relative/path/"),
    ):
        overridable_public_settings.core.PUBLIC_URL = public_url  # type: ignore [assignment]
        overridable_public_settings.PUBLIC_API_PREFIX = public_api_prefix

        assert account_holder.marketing_opt_out_link == expected_url
