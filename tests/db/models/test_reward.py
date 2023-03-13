from datetime import UTC, datetime, timedelta
from uuid import uuid4

from cosmos.db.models import Campaign, Reward, RewardConfig
from tests.conftest import SetupType


def test_reward_status_prop_unallocated(setup: SetupType, reward_config: RewardConfig, campaign: Campaign) -> None:
    db_session, retailer, _ = setup
    reward = Reward(
        account_holder_id=None,
        issued_date=None,
        expiry_date=None,
        cancelled_date=None,
        redeemed_date=None,
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
    )
    db_session.add(reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.UNALLOCATED


def test_reward_status_prop_issued(setup: SetupType, reward_config: RewardConfig, campaign: Campaign) -> None:
    db_session, retailer, account_holder = setup

    now = datetime.now(tz=UTC)

    reward = Reward(
        account_holder_id=account_holder.id,
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
        issued_date=now,
        expiry_date=now + timedelta(days=10),
    )
    db_session.add(reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.ISSUED


def test_reward_status_prop_issued_and_expired(
    setup: SetupType, reward_config: RewardConfig, campaign: Campaign
) -> None:
    db_session, retailer, account_holder = setup

    now = datetime.now(tz=UTC)

    reward = Reward(
        account_holder_id=account_holder.id,
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
        issued_date=now,
        expiry_date=now - timedelta(days=10),
    )
    db_session.add(reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.EXPIRED


def test_reward_status_prop_redeemed(setup: SetupType, reward_config: RewardConfig, campaign: Campaign) -> None:
    db_session, retailer, account_holder = setup

    now = datetime.now(tz=UTC)

    reward = Reward(
        account_holder_id=account_holder.id,
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
        issued_date=now - timedelta(days=5),
        redeemed_date=now - timedelta(days=1),
        expiry_date=now + timedelta(days=-10),
    )
    db_session.add(reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.REDEEMED


def test_reward_status_prop_cancelled(setup: SetupType, reward_config: RewardConfig, campaign: Campaign) -> None:
    db_session, retailer, account_holder = setup

    now = datetime.now(tz=UTC)

    reward = Reward(
        account_holder_id=account_holder.id,
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
        issued_date=now - timedelta(days=5),
        cancelled_date=now - timedelta(days=1),
        expiry_date=now + timedelta(days=10),
    )
    db_session.add(reward)
    db_session.commit()

    assert reward.status == Reward.RewardStatuses.CANCELLED
