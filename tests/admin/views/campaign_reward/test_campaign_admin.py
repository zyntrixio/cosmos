from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpretty
import pytest

from bs4 import BeautifulSoup
from deepdiff import DeepDiff
from flask import url_for

from admin.views.campaign_reward.campaign import CampaignAdmin
from cosmos.campaigns.api.schemas import CampaignsMigrationSchema, CampaignsStatusChangeSchema
from cosmos.campaigns.config import campaign_settings
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.rewards.enums import PendingRewardActions, PendingRewardMigrationActions

if TYPE_CHECKING:
    from collections.abc import Callable

    from flask.testing import FlaskClient
    from pytest_mock import MockerFixture

    from cosmos.db.models import Campaign, Retailer


@httpretty.activate
def test_campaign_end_action_migration_ok(
    test_client: "FlaskClient", create_campaign: "Callable[..., Campaign]", mocker: "MockerFixture"
) -> None:

    active_campaign = create_campaign(slug="from-campaign")
    draft_campaign = create_campaign(slug="to-campaign", status="DRAFT")

    mock_status_chang_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_status_change_request")
    httpretty.register_uri(
        "POST",
        f"{campaign_settings.CAMPAIGN_BASE_URL}/{active_campaign.retailer.slug}/migration",
        body="OK",
        status=200,
    )

    migration_action = PendingRewardMigrationActions.TRANSFER
    transfer_balance = True
    convert_rate = 80
    qualify_threshold = 20

    # check expected form fields are present and set Session cookie.
    resp_get = test_client.get(
        url_for("campaigns.end_campaigns", ids=[active_campaign.id, draft_campaign.id]),
        follow_redirects=True,
    )

    assert resp_get.status_code == 200
    assert (form := BeautifulSoup(resp_get.text).form)
    assert form.find(id="handle_pending_rewards")
    assert form.find(id="transfer_balance")
    assert form.find(id="convert_rate")
    assert form.find(id="qualify_threshold")

    resp_post = test_client.post(
        url_for("campaigns.end_campaigns", ids=[active_campaign.id, draft_campaign.id]),
        data={
            "handle_pending_rewards": migration_action.value,
            "transfer_balance": transfer_balance,
            "convert_rate": convert_rate,
            "qualify_threshold": qualify_threshold,
        },
        follow_redirects=True,
    )

    assert resp_post.status_code == 200

    mock_status_chang_fn.assert_not_called()

    assert (payload := CampaignsMigrationSchema(**httpretty.last_request().parsed_body))
    assert not DeepDiff(
        payload.dict(),
        {
            "to_campaign": draft_campaign.slug,
            "from_campaign": active_campaign.slug,
            "pending_rewards_action": migration_action,
            "balance_action": {
                "transfer": transfer_balance,
                "conversion_rate": convert_rate,
                "qualifying_threshold": qualify_threshold,
            },
            "activity_metadata": {
                "sso_username": "Test User",
            },
        },
    )


@httpretty.activate
def test_campaign_end_action_status_change_ok(
    test_client: "FlaskClient", create_campaign: "Callable[..., Campaign]", mocker: "MockerFixture"
) -> None:

    active_campaign = create_campaign(slug="from-campaign")

    mock_migration_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_migration_request")
    httpretty.register_uri(
        "POST",
        f"{campaign_settings.CAMPAIGN_BASE_URL}/{active_campaign.retailer.slug}/status-change",
        body="OK",
        status=200,
    )

    migration_action = PendingRewardActions.REMOVE

    # check expected form fields are present and set Session cookie.
    resp_get = test_client.get(
        url_for("campaigns.end_campaigns", ids=[active_campaign.id]),
        follow_redirects=True,
    )

    assert resp_get.status_code == 200
    assert (form := BeautifulSoup(resp_get.text).form)
    assert form.find(id="handle_pending_rewards")
    assert not form.find(id="transfer_balance")
    assert not form.find(id="convert_rate")
    assert not form.find(id="qualify_threshold")

    resp_post = test_client.post(
        url_for("campaigns.end_campaigns", ids=[active_campaign.id]),
        data={
            "handle_pending_rewards": migration_action.value,
        },
        follow_redirects=True,
    )

    assert resp_post.status_code == 200
    mock_migration_fn.assert_not_called()

    assert (payload := CampaignsStatusChangeSchema(**httpretty.last_request().parsed_body))
    assert not DeepDiff(
        payload.dict(),
        {
            "requested_status": CampaignStatuses.ENDED,
            "campaign_slug": active_campaign.slug,
            "pending_rewards_action": migration_action,
            "activity_metadata": {
                "sso_username": "Test User",
            },
        },
    )


def test_campaign_end_action_too_many_ids(
    test_client: "FlaskClient", create_campaign: "Callable[..., Campaign]", mocker: "MockerFixture"
) -> None:

    mock_status_chang_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_status_change_request")
    mock_migration_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_migration_request")

    campaigns = [create_campaign(slug=f"from-campaign-{i}") for i in range(3)]

    resp = test_client.get(
        url_for("campaigns.end_campaigns", ids=[cmp.id for cmp in campaigns]),
        follow_redirects=True,
    )

    assert resp.status_code == 200
    # check that we have been redirected to the index page
    assert urlparse(resp.request.url).path == url_for("campaigns.index_view")
    # check that we got the expected flashed error
    assert (
        "Only up to one DRAFT and one ACTIVE campaign allowed"
        in BeautifulSoup(resp.text).find_all("div", {"class": "alert alert-danger alert-dismissable"})[0].text
    )
    mock_status_chang_fn.assert_not_called()
    mock_migration_fn.assert_not_called()


@pytest.mark.parametrize(
    ("to_campaign_status", "from_campaign_status", "expected_msg"),
    (
        pytest.param("ACTIVE", "ACTIVE", "Only up to one DRAFT and one ACTIVE campaign allowed", id="both active"),
        pytest.param("DRAFT", "DRAFT", "One ACTIVE campaign must be provided", id="both draft / no active provided"),
        pytest.param("ACTIVE", "ENDED", "Only ACTIVE or DRAFT campaigns allowed for this action", id="one ended"),
        pytest.param(
            "CANCELLED", "DRAFT", "Only ACTIVE or DRAFT campaigns allowed for this action", id="one cancelled"
        ),
    ),
)
def test_campaign_end_action_wrong_status(
    to_campaign_status: str,
    from_campaign_status: str,
    expected_msg: str,
    test_client: "FlaskClient",
    create_campaign: "Callable[..., Campaign]",
    mocker: "MockerFixture",
) -> None:

    mock_status_chang_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_status_change_request")
    mock_migration_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_migration_request")

    campaign_1 = create_campaign(slug="from-campaign", status=from_campaign_status)
    campaign_2 = create_campaign(slug="to-campaign", status=to_campaign_status)

    resp = test_client.get(
        url_for("campaigns.end_campaigns", ids=[campaign_1.id, campaign_2.id]),
        follow_redirects=True,
    )

    assert resp.status_code == 200
    # check that we have been redirected to the index page
    assert urlparse(resp.request.url).path == url_for("campaigns.index_view")
    # check that we got the expected flashed error
    assert (
        expected_msg
        in BeautifulSoup(resp.text).find_all("div", {"class": "alert alert-danger alert-dismissable"})[0].text
    )
    mock_status_chang_fn.assert_not_called()
    mock_migration_fn.assert_not_called()


def test_campaign_end_action_different_retailer(
    test_client: "FlaskClient",
    create_campaign: "Callable[..., Campaign]",
    create_retailer: "Callable[..., Retailer]",
    mocker: "MockerFixture",
) -> None:

    mock_status_chang_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_status_change_request")
    mock_migration_fn = mocker.patch.object(CampaignAdmin, "_send_campaign_migration_request")
    retailer_2 = create_retailer(slug="retailer-2")

    campaign_1 = create_campaign(slug="from-campaign", status="ACTIVE")
    campaign_2 = create_campaign(slug="to-campaign", status="DRAFT", retailer_id=retailer_2.id)

    resp = test_client.get(
        url_for("campaigns.end_campaigns", ids=[campaign_1.id, campaign_2.id]),
        follow_redirects=True,
    )

    assert resp.status_code == 200
    # check that we have been redirected to the index page
    assert urlparse(resp.request.url).path == url_for("campaigns.index_view")
    # check that we got the expected flashed error
    assert (
        "Selected campaigns must belong to the same retailer."
        in BeautifulSoup(resp.text).find_all("div", {"class": "alert alert-danger alert-dismissable"})[0].text
    )
    mock_status_chang_fn.assert_not_called()
    mock_migration_fn.assert_not_called()
