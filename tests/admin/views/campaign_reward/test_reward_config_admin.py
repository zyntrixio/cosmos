from typing import TYPE_CHECKING

import pytest

from sqlalchemy import func
from sqlalchemy.future import select

from cosmos.campaigns.enums import CampaignStatuses
from cosmos.db.models import RewardConfig
from cosmos.rewards.enums import RewardTypeStatuses

if TYPE_CHECKING:
    from flask.testing import FlaskClient
    from sqlalchemy.orm import Session

    from cosmos.db.models import Campaign, FetchType, Retailer


def test_reward_config_deactivate_action_too_many_objects(
    db_session: "Session",
    test_client: "FlaskClient",
    retailer: "Retailer",
    reward_config: RewardConfig,
    pre_loaded_fetch_type: "FetchType",
) -> None:
    rc = RewardConfig(
        id=100,
        slug="reward-config-100",
        status=RewardTypeStatuses.ACTIVE,
        fetch_type_id=pre_loaded_fetch_type.id,
    )
    retailer.reward_configs.append(rc)
    db_session.commit()
    resp = test_client.post(
        "/admin/campaign-and-reward/reward-configs/action/",
        data={
            "url": "/admin/campaign-and-reward/reward-configs/",
            "action": "deactivate-reward-type",
            "rowid": ["1", "100"],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert "This action must be completed for reward_configs one at a time" in resp.text
    assert (
        db_session.scalar(
            select(func.count("*"))
            .select_from(RewardConfig)
            .where(RewardConfig.retailer_id == retailer.id, RewardConfig.status == RewardTypeStatuses.ACTIVE)
        )
        == 2
    )


@pytest.mark.parametrize(
    "campaign_status",
    [CampaignStatuses.ACTIVE, CampaignStatuses.CANCELLED, CampaignStatuses.DRAFT, CampaignStatuses.ENDED],
)
def test_reward_config_deactivate_action(
    campaign_status: CampaignStatuses,
    db_session: "Session",
    test_client: "FlaskClient",
    campaign_with_rules: "Campaign",
) -> None:

    campaign_with_rules.status = campaign_status
    db_session.commit()

    reward_config = campaign_with_rules.reward_rule.reward_config
    resp = test_client.post(
        "/admin/campaign-and-reward/reward-configs/action/",
        data={
            "url": "/admin/campaign-and-reward/reward-configs/",
            "action": "deactivate-reward-type",
            "rowid": f"{reward_config.id}",
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    db_session.refresh(reward_config)

    if campaign_status == CampaignStatuses.ACTIVE:
        assert reward_config.status == RewardTypeStatuses.ACTIVE
        assert "This RewardConfig has ACTIVE campaigns associated with it" in resp.text
    else:
        assert reward_config.status == RewardTypeStatuses.DELETED
        assert "RewardConfig DEACTIVATED" in resp.text
