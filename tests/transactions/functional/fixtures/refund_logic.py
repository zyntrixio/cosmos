import uuid

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import pytest

from cosmos.accounts.activity.enums import ActivityType as AccountActivityType
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.transactions.activity.enums import ActivityType

if TYPE_CHECKING:
    from enum import Enum

# asyncpg can't handle timezone aware to naive conversion, update this once we move to psycopg3
now = datetime.now(tz=timezone.utc).replace(tzinfo=None)
first_uuid = uuid.uuid4()
second_uuid = uuid.uuid4()


@dataclass
class PendingRewardData:
    created_date: datetime
    conversion_date: datetime
    count: int
    value: int
    total_cost_to_user: int
    pending_reward_uuid: uuid.UUID


@dataclass
class SetupData:
    balance: int
    adjustment: int
    pending_rewards: list[PendingRewardData]


@dataclass
class ExpectationData:
    balance: int
    pending_rewards: list[PendingRewardData]
    activities: list[tuple["Enum", int]]


test_refund_data = (
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-5000,
            pending_rewards=[],
        ),
        ExpectationData(
            balance=0,
            pending_rewards=[],
            activities=[
                (ActivityType.REFUND_NOT_RECOUPED, 1),
                (AccountActivityType.BALANCE_CHANGE, 1),
            ],
        ),
        id="refund, insufficient balance & no PRR -> 0 balance",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-25000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=1,
                    value=20000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=first_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=0,
            pending_rewards=[],
            activities=[
                (ActivityType.REFUND_NOT_RECOUPED, 1),
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 1),
            ],
        ),
        id="refund, balance + 1 PRR -> no balance, refund too big",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-47000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=25000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=0,
            pending_rewards=[],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 2),
            ],
        ),
        id="refund, 2 PRRs, count 2 -> nothing, refund too big across multiple records",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-5000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=50000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=45000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_UPDATE, 1),
            ],
        ),
        id="refund, slush >= refund, 2 PRRs only 1 w/ slush",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-5000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=50000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=24500,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=45000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=24500,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_UPDATE, 1),
            ],
        ),
        id="refund, slush >= refund, 2 PRRs both w/ slush but only 1 w/ slush >= refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-5000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=50000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=25000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=50000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_UPDATE, 1),
            ],
        ),
        id="refund, slush >= refund, 2 PRRs both w/ slush >= refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-1500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=31000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20500,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_UPDATE, 2),
            ],
        ),
        id="refund, combined slush == refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-1500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=31000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=21000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30500,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_UPDATE, 2),
            ],
        ),
        id="refund, combined slush > refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-2500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=31000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20500,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=1000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_UPDATE, 2),
            ],
        ),
        id="refund, combined slush < refund but combined slush + balance > refund",
    ),
    pytest.param(
        SetupData(
            balance=500,
            adjustment=-2500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=31000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=21000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=0,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_UPDATE, 2),
            ],
        ),
        id="refund, combined slush < refund but combined slush + balance == refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-11000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                )
            ],
        ),
        ExpectationData(
            balance=1000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=first_uuid,
                )
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_UPDATE, 1),
            ],
        ),
        id="refund, combined slush < refund but combined slush + balance > refund, 1 PRR w/ large slush",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-15000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=7000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=1,
                    value=10000,
                    total_cost_to_user=10000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 1),
            ],
        ),
        id="refund, combined slush + balance < refund, 1 PRR w/slush -> 1 PRR count decreased",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-15000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=35000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=25000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=7000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=1,
                    value=10000,
                    total_cost_to_user=10000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 1),
                (RewardsActivityType.REWARD_UPDATE, 1),
            ],
        ),
        id="refund, combined slush + balance < refund, 2 PRRs w/slush -> 1 PRR count decreased",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-40000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=27000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=25000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=4000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=1,
                    value=10000,
                    total_cost_to_user=10000,
                    pending_reward_uuid=first_uuid,
                )
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 2),
            ],
        ),
        id=(
            "refund, combined slush + balance < refund, 2 PRRs w/slush, very large refund"
            " -> 1 PRR removed & 1 PRR count decreased"
        ),
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-1500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
            ],
        ),
        id="refund, 2 PRRs w/o slush, balance > refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-1500,
            pending_rewards=[],
        ),
        ExpectationData(
            balance=500,
            pending_rewards=[],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
            ],
        ),
        id="refund, no PRRs, balance > refund",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-12500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=9500,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
            ],
            activities=[
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 1),
            ],
        ),
        id="refund, 2 PRRs w/o slush, balance < refund, 1 PRR removed & balance transfered",
    ),
    pytest.param(
        SetupData(
            balance=2000,
            adjustment=-30000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=3,
                    value=10000,
                    total_cost_to_user=30000,
                    pending_reward_uuid=first_uuid,
                ),
                PendingRewardData(
                    created_date=now,
                    conversion_date=now + timedelta(days=5),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=second_uuid,
                ),
            ],
        ),
        ExpectationData(
            balance=2000,
            pending_rewards=[
                PendingRewardData(
                    created_date=now - timedelta(days=1),
                    conversion_date=now + timedelta(days=4),
                    count=2,
                    value=10000,
                    total_cost_to_user=20000,
                    pending_reward_uuid=first_uuid,
                ),
            ],
            activities=[
                (RewardsActivityType.REWARD_STATUS, 2),
            ],
        ),
        id="refund, 2 PRRs w/o slush, balance < refund, very large refund -> 1 PRR removed & 1 PRR count decreased",
    ),
)
