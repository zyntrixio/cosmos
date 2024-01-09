from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpretty
import pytest

from bs4 import BeautifulSoup
from deepdiff import DeepDiff
from flask import url_for
from sqlalchemy.future import select

from admin.views.campaign_reward.campaign import CampaignAdmin
from cosmos.campaigns.api.schemas import CampaignsMigrationSchema, CampaignsStatusChangeSchema
from cosmos.campaigns.config import campaign_settings
from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import Campaign
from cosmos.rewards.enums import PendingRewardActions, PendingRewardMigrationActions

if TYPE_CHECKING:
    from collections.abc import Callable

    from flask.testing import FlaskClient
    from pytest_mock import MockerFixture
    from sqlalchemy.orm import Session

    from cosmos.db.models import Retailer


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


def test_campaign_clone_action_ok(
    db_session: "Session",
    test_client: "FlaskClient",
    create_campaign: "Callable[..., Campaign]",
    mocker: "MockerFixture",
) -> None:
    test_campaign = create_campaign(slug="test-campaign")
    mock_send_activity = mocker.patch("admin.views.campaign_reward.campaign.sync_send_activity")

    # check expected form fields are present and set Session cookie.
    resp = test_client.post(
        url_for("campaigns.action_view"),
        data={
            "url": url_for("campaigns.index_view"),
            "action": "clone-campaign",
            "rowid": [test_campaign.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert mock_send_activity.call_count == 3

    cloned_campaign = db_session.execute(
        select(Campaign).where(Campaign.slug == f"CLONE_{test_campaign.slug}")
    ).scalar_one_or_none()

    assert cloned_campaign
    assert cloned_campaign.reward_rule
    assert cloned_campaign.earn_rule
    assert cloned_campaign.status == CampaignStatuses.DRAFT


@pytest.mark.parametrize(
    "missing_rule",
    (
        pytest.param("earn_rule", id="missing earn rule"),
        pytest.param("reward_rule", id="missing reward rule"),
    ),
)
def test_campaign_clone_action_missing_rule(
    missing_rule: str,
    db_session: "Session",
    test_client: "FlaskClient",
    create_campaign: "Callable[..., Campaign]",
    mocker: "MockerFixture",
) -> None:
    test_campaign = create_campaign(slug="test-campaign")

    db_session.delete(getattr(test_campaign, missing_rule))
    db_session.commit()

    mock_send_activity = mocker.patch("admin.views.campaign_reward.campaign.sync_send_activity")
    mock_flash = mocker.patch("admin.views.campaign_reward.campaign.flash")

    # check expected form fields are present and set Session cookie.
    resp = test_client.post(
        url_for("campaigns.action_view"),
        data={
            "url": url_for("campaigns.index_view"),
            "action": "clone-campaign",
            "rowid": [test_campaign.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mock_flash.assert_called_once_with("Unable to clone, missing earn or reward rule.", category="error")
    mock_send_activity.assert_not_called()
    assert not db_session.execute(
        select(Campaign).where(Campaign.slug == f"CLONE_{test_campaign.slug}")
    ).scalar_one_or_none()


def test_campaign_clone_action_resulting_slug_exists_already(
    test_client: "FlaskClient", create_campaign: "Callable[..., Campaign]", mocker: "MockerFixture"
) -> None:
    test_campaign = create_campaign(slug="test-campaign")
    cloned_campaign = create_campaign(slug="CLONE_test-campaign")

    mock_send_activity = mocker.patch("admin.views.campaign_reward.campaign.sync_send_activity")
    mock_flash = mocker.patch("admin.views.campaign_reward.campaign.flash")

    # check expected form fields are present and set Session cookie.
    resp = test_client.post(
        url_for("campaigns.action_view"),
        data={
            "url": url_for("campaigns.index_view"),
            "action": "clone-campaign",
            "rowid": [test_campaign.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mock_flash.assert_called_once_with(
        f"Another campaign with slug '{cloned_campaign.slug}' already exists, "
        "please update it before trying to clone this campaign again.",
        category="error",
    )
    mock_send_activity.assert_not_called()


def test_campaign_clone_action_resulting_slug_too_long(
    test_client: "FlaskClient", create_campaign: "Callable[..., Campaign]", mocker: "MockerFixture"
) -> None:
    # setup a slug which is 99 char long
    test_campaign = create_campaign(slug="test-campaign" + ("-" * 82))

    resulting_slug = f"CLONE_{test_campaign.slug}"

    assert len(resulting_slug) > 100

    mock_send_activity = mocker.patch("admin.views.campaign_reward.campaign.sync_send_activity")
    mock_flash = mocker.patch("admin.views.campaign_reward.campaign.flash")

    # check expected form fields are present and set Session cookie.
    resp = test_client.post(
        url_for("campaigns.action_view"),
        data={
            "url": url_for("campaigns.index_view"),
            "action": "clone-campaign",
            "rowid": [test_campaign.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mock_flash.assert_called_once_with(
        f"Cloned campaign slug '{resulting_slug}' would exceed max slug length of 100 characters.",
        category="error",
    )
    mock_send_activity.assert_not_called()


@pytest.mark.parametrize("status", (CampaignStatuses.ACTIVE, CampaignStatuses.CANCELLED, CampaignStatuses.ENDED))
def test_delete_campaign_not_ok(
    status: CampaignStatuses, db_session: "Session", test_client: "FlaskClient", campaign_with_rules: Campaign
) -> None:
    campaign_with_rules.status = status
    db_session.commit()

    resp = test_client.post(
        f"{url_for('campaigns.delete_view')}?id={campaign_with_rules.id}",
        follow_redirects=True,
    )
    db_session.refresh(campaign_with_rules)
    assert campaign_with_rules.status == status
    assert "Cannot delete campaigns that are not DRAFT" in resp.text


def test_delete_campaign_ok(db_session: "Session", test_client: "FlaskClient", campaign_with_rules: Campaign) -> None:
    campaign_id = campaign_with_rules.id
    campaign_with_rules.status = CampaignStatuses.DRAFT
    db_session.commit()

    resp = test_client.post(
        f"{url_for('campaigns.delete_view')}?id={campaign_with_rules.id}",
        follow_redirects=True,
    )
    assert "Record was successfully deleted" in resp.text
    db_session.expunge(campaign_with_rules)
    assert not db_session.get(Campaign, campaign_id)
