from dataclasses import dataclass
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from flask import url_for
from sqlalchemy import func
from sqlalchemy.future import select

from cosmos.db.models import Reward

if TYPE_CHECKING:

    from collections.abc import Callable

    from flask.testing import FlaskClient
    from pytest_mock import MockerFixture

    from cosmos.db.models import Retailer
    from tests.conftest import SetupType


@dataclass
class Mocks:
    send_activity: MagicMock
    flash: MagicMock


@pytest.fixture(scope="function")
def mocks(mocker: "MockerFixture") -> Mocks:
    return Mocks(
        send_activity=mocker.patch("admin.views.campaign_reward.reward.sync_send_activity"),
        flash=mocker.patch("admin.views.campaign_reward.reward.flash"),
    )


def test_delete_reward_ok(
    setup: "SetupType", test_client: "FlaskClient", create_mock_reward: "Callable[..., Reward]", mocks: Mocks
) -> None:
    db_session, retailer, _ = setup

    rewards = [create_mock_reward(retailer_id=retailer.id, code=str(uuid4())) for _ in range(3)]
    rewards_ids = [rwd.id for rwd in rewards]
    resp = test_client.post(
        url_for("rewards.action_view"),
        data={
            "url": url_for("rewards.index_view"),
            "action": "delete-rewards",
            "rowid": rewards_ids,
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mocks.send_activity.assert_called_once()
    mocks.flash.assert_called_once_with("Successfully deleted selected rewards")

    assert not db_session.scalars(select(Reward).where(Reward.id.in_(rewards_ids))).all()


def test_delete_reward_different_retailers(
    setup: "SetupType",
    test_client: "FlaskClient",
    create_retailer: "Callable[..., Retailer]",
    create_mock_reward: "Callable[..., Reward]",
    mocks: Mocks,
) -> None:
    db_session, retailer, _ = setup
    retailer_2 = create_retailer(slug="retailer-2")

    reward_1 = create_mock_reward(retailer_id=retailer.id, code=str(uuid4()))
    reward_2 = create_mock_reward(retailer_id=retailer_2.id, code=str(uuid4()))

    rewards_ids = [reward_1.id, reward_2.id]
    resp = test_client.post(
        url_for("rewards.action_view"),
        data={
            "url": url_for("rewards.index_view"),
            "action": "delete-rewards",
            "rowid": rewards_ids,
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mocks.send_activity.assert_not_called()
    mocks.flash.assert_called_once_with("Not all selected rewards are for the same retailer", category="error")

    assert db_session.scalar(select(func.count(Reward.id)).where(Reward.id.in_(rewards_ids))) == 2


def test_delete_reward_issued_reward(
    setup: "SetupType",
    test_client: "FlaskClient",
    create_mock_reward: "Callable[..., Reward]",
    mocks: Mocks,
) -> None:
    db_session, retailer, account_holder = setup

    reward = create_mock_reward(retailer_id=retailer.id, account_holder_id=account_holder.id, code=str(uuid4()))

    resp = test_client.post(
        url_for("rewards.action_view"),
        data={
            "url": url_for("rewards.index_view"),
            "action": "delete-rewards",
            "rowid": [reward.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mocks.send_activity.assert_not_called()
    mocks.flash.assert_called_once_with("Not all selected rewards are eligible for deletion", category="error")

    assert db_session.scalar(select(func.count(Reward.id)).where(Reward.id == reward.id))


def test_delete_reward_deleted_reward(
    setup: "SetupType",
    test_client: "FlaskClient",
    create_mock_reward: "Callable[..., Reward]",
    mocks: Mocks,
) -> None:
    db_session, retailer, _ = setup

    reward = create_mock_reward(retailer_id=retailer.id, deleted=True, code=str(uuid4()))

    resp = test_client.post(
        url_for("rewards.action_view"),
        data={
            "url": url_for("rewards.index_view"),
            "action": "delete-rewards",
            "rowid": [reward.id],
        },
        follow_redirects=True,
    )
    assert resp.status_code == 200
    mocks.send_activity.assert_not_called()
    mocks.flash.assert_called_once_with("Not all selected rewards are eligible for deletion", category="error")

    assert db_session.scalar(select(func.count(Reward.id)).where(Reward.id == reward.id))
