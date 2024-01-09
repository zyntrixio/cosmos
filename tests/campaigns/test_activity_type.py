import uuid

from datetime import UTC, datetime

from pytest_mock import MockerFixture, MockFixture

from cosmos.campaigns.activity.enums import ActivityType
from cosmos.campaigns.enums import CampaignStatuses, LoyaltyTypes
from cosmos.rewards.enums import PendingRewardMigrationActions


def test_get_campaign_status_change_activity_data(mocker: MockerFixture) -> None:
    fake_now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = fake_now

    payload = ActivityType.get_campaign_status_change_activity_data(
        updated_at=fake_now,
        campaign_name="Test Campaign",
        campaign_slug="test-campaign",
        retailer_slug="test-retailer",
        original_status=CampaignStatuses.DRAFT,
        new_status=CampaignStatuses.ACTIVE,
        sso_username="testuser",
    )
    assert payload == {
        "type": ActivityType.CAMPAIGN.name,
        "datetime": fake_now,
        "underlying_datetime": fake_now,
        "summary": f"Test Campaign {CampaignStatuses.ACTIVE.value}",
        "reasons": [],
        "activity_identifier": "test-campaign",
        "user_id": "testuser",
        "associated_value": str(CampaignStatuses.ACTIVE.value),
        "retailer": "test-retailer",
        "campaigns": ["test-campaign"],
        "data": {
            "campaign": {
                "new_values": {
                    "status": str(CampaignStatuses.ACTIVE.value),
                },
                "original_values": {
                    "status": str(CampaignStatuses.DRAFT.value),
                },
            }
        },
    }


def test_get_campaign_migration_activity_data(mocker: MockFixture) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=UTC)
    mock_datetime.now.return_value = fake_now

    user_name = "Jane Doe"
    retailer_slug = "test-retailer"
    activity_datetime = datetime.now(tz=UTC)
    pending_rewards = PendingRewardMigrationActions.CONVERT.value
    from_campaign_slug = "test-campaign"
    to_campaign_slug = "second-test-campaign"

    payload = ActivityType.get_campaign_migration_activity_data(
        retailer_slug=retailer_slug,
        from_campaign_slug=from_campaign_slug,
        to_campaign_slug=to_campaign_slug,
        sso_username=user_name,
        activity_datetime=activity_datetime,
        balance_conversion_rate=80,
        qualify_threshold=20,
        pending_rewards=pending_rewards,
        transfer_balance_requested=True,
    )

    assert payload == {
        "type": ActivityType.CAMPAIGN_MIGRATION.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": (
            f"{retailer_slug} Campaign {from_campaign_slug} has ended"
            f" and account holders have been migrated to Campaign {to_campaign_slug}"
        ),
        "reasons": [f"Campaign {from_campaign_slug} was ended"],
        "activity_identifier": retailer_slug,
        "user_id": user_name,
        "associated_value": "N/A",
        "retailer": retailer_slug,
        "campaigns": [from_campaign_slug, to_campaign_slug],
        "data": {
            "ended_campaign": from_campaign_slug,
            "activated_campaign": to_campaign_slug,
            "balance_conversion_rate": "80%",
            "qualify_threshold": "20%",
            "pending_rewards": pending_rewards.lower(),
        },
    }


def test_get_balance_change_activity_data(mocker: MockFixture) -> None:
    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    fake_now = datetime.now(tz=UTC)
    mock_datetime.now.return_value = fake_now
    retailer_slug = "test-retailer"
    activity_datetime = datetime.now(tz=UTC)
    from_campaign_slug = "test-campaign"
    to_campaign_slug = "second-test-campaign"
    account_holder_uuid = str(uuid.uuid4())

    payload = ActivityType.get_balance_change_activity_data(
        retailer_slug=retailer_slug,
        from_campaign_slug=from_campaign_slug,
        to_campaign_slug=to_campaign_slug,
        account_holder_uuid=account_holder_uuid,
        activity_datetime=activity_datetime,
        new_balance=5000,
        loyalty_type=LoyaltyTypes.ACCUMULATOR,
    )

    assert payload == {
        "type": ActivityType.BALANCE_CHANGE.name,
        "datetime": fake_now,
        "underlying_datetime": activity_datetime,
        "summary": (f"{retailer_slug} {to_campaign_slug} Balance £50.00"),
        "reasons": [f"Migrated from ended campaign {from_campaign_slug}"],
        "activity_identifier": "N/A",
        "user_id": account_holder_uuid,
        "associated_value": "£50.00",
        "retailer": retailer_slug,
        "campaigns": [to_campaign_slug],
        "data": {
            "loyalty_type": LoyaltyTypes.ACCUMULATOR.name,
            "new_balance": 5000,
            "original_balance": 0,
        },
    }
