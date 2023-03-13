import uuid

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from cosmos.accounts.activity.enums import ActivityType as AccountActivityType
from cosmos.rewards.activity.enums import ActivityType as RewardsActivityType
from cosmos.transactions.activity.enums import ActivityType as TransactionActivityType

if TYPE_CHECKING:
    from enum import Enum

# asyncpg can't handle timezone aware to naive conversion, update this once we move to psycopg3
now = datetime.now(tz=UTC).replace(tzinfo=None)
first_uuid = uuid.uuid4()
second_uuid = uuid.uuid4()

canned_transaction_id = uuid.uuid4()
canned_account_holder_uuid = uuid.uuid4()


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
    activity_payloads: list[dict] | None = None


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
                (TransactionActivityType.REFUND_NOT_RECOUPED, 1),
                (AccountActivityType.BALANCE_CHANGE, 1),
            ],
            activity_payloads=[
                {
                    "activity_type": TransactionActivityType.REFUND_NOT_RECOUPED,
                    "payload_formatter_fn": TransactionActivityType.get_refund_not_recouped_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "adjustment": -5000,
                        "amount_not_recouped": 3000,
                        "amount_recouped": 0,
                        "campaigns": ["test-campaign"],
                        "retailer": mock.ANY,
                        "transaction_id": str(canned_transaction_id),
                    },
                },
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 0,
                        "original_balance": 2000,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£50.00",
                    },
                },
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
                (TransactionActivityType.REFUND_NOT_RECOUPED, 1),
                (AccountActivityType.BALANCE_CHANGE, 1),
                (RewardsActivityType.REWARD_STATUS, 1),
            ],
            activity_payloads=[
                {
                    "activity_type": TransactionActivityType.REFUND_NOT_RECOUPED,
                    "payload_formatter_fn": TransactionActivityType.get_refund_not_recouped_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "adjustment": -25000,
                        "amount_not_recouped": 3000,
                        "amount_recouped": 0,
                        "campaigns": ["test-campaign"],
                        "retailer": mock.ANY,
                        "transaction_id": str(canned_transaction_id),
                    },
                },
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "summary": "Test Retailer - test-campaign: -£250.00",
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "campaigns": ["test-campaign"],
                        "new_balance": 0,
                        "original_balance": 2000,
                        "retailer_slug": "re-test",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "count": 1,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                        }
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "retailer_slug": "re-test",
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "summary": "Test Retailer - test-campaign: -£470.00",
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "new_balance": 0,
                        "original_balance": 2000,
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "new_status": "deleted",
                            "original_status": "pending",
                            "count": 2,
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "new_status": "deleted",
                            "original_status": "pending",
                            "count": 2,
                        },
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 45000,
                                "original_total_cost_to_user": 50000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        }
                    ],
                }
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 45000,
                                "original_total_cost_to_user": 50000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        }
                    ],
                }
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 20000,
                                "original_total_cost_to_user": 25000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        }
                    ],
                }
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 19500,
                                "original_total_cost_to_user": 20500,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 30000,
                                "original_total_cost_to_user": 31000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                    ],
                }
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 20500,
                                "original_total_cost_to_user": 21000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_ad - a bit tricky to get
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 30500,
                                "original_total_cost_to_user": 31000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                    ],
                }
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 1000,
                        "original_balance": 2000,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£25.00",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 18500,
                                "original_total_cost_to_user": 20500,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 30000,
                                "original_total_cost_to_user": 31000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 0,
                        "original_balance": 500,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£25.00",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 19500,
                                "original_total_cost_to_user": 21000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 30500,
                                "original_total_cost_to_user": 31000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 1000,
                        "original_balance": 2000,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£110.00",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 29000,
                                "original_total_cost_to_user": 30000,
                            },
                            "summary": "Pending Reward Record's total cost to user updated",
                        },
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 7000,
                        "original_balance": 2000,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£150.00",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending_reward_uuid - bit tricky to get
                            "campaigns": ["test-campaign"],
                            "count": 1,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                        }
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "campaigns": ["test-campaign"],
                        "new_balance": 7000,
                        "original_balance": 2000,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£150.00",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending_reward_uuid - bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward removed due to refund",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                            "count": 1,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "retailer_slug": "re-test",
                        }
                    ],
                },
                {
                    "activity_type": RewardsActivityType.REWARD_UPDATE,
                    "payload_formatter_fn": RewardsActivityType.get_reward_update_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": mock.ANY,  # pending reward updated_at - bit tricky to get
                            "activity_identifier": mock.ANY,  # pending_reward_uuid - bit tricky to get
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward updated due to refund",
                            "summary": "Pending Reward Record's total cost to user updated",
                            "retailer_slug": "re-test",
                            "reward_update_data": {
                                "new_total_cost_to_user": 30000,
                                "original_total_cost_to_user": 35000,
                            },
                        }
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "summary": "Test Retailer - test-campaign: -£400.00",
                        "campaigns": ["test-campaign"],
                        "new_balance": 4000,
                        "original_balance": 2000,
                        "retailer_slug": "re-test",
                    },
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward removed due to refund",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                            "count": 2,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "retailer_slug": "re-test",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,
                            "campaigns": ["test-campaign"],
                            "reason": "Pending Reward removed due to refund",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                            "count": 1,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "retailer_slug": "re-test",
                        },
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "summary": "Test Retailer - test-campaign: -£15.00",
                        "campaigns": ["test-campaign"],
                        "new_balance": 500,
                        "original_balance": 2000,
                        "retailer_slug": "re-test",
                    },
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "activity_datetime": now,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                        "summary": "Test Retailer - test-campaign: -£15.00",
                        "campaigns": ["test-campaign"],
                        "new_balance": 500,
                        "original_balance": 2000,
                        "retailer_slug": "re-test",
                    },
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                },
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
            activity_payloads=[
                {
                    "activity_type": AccountActivityType.BALANCE_CHANGE,
                    "payload_formatter_fn": AccountActivityType.get_balance_change_activity_data,
                    "formatter_kwargs": {
                        "account_holder_uuid": canned_account_holder_uuid,
                        "retailer_slug": "re-test",
                        "summary": "Test Retailer - test-campaign: -£125.00",
                        "original_balance": 2000,
                        "new_balance": 9500,
                        "campaigns": ["test-campaign"],
                        "activity_datetime": now,
                        "reason": f"Refund transaction id: {canned_transaction_id}",
                    },
                },
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "count": 2,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                        }
                    ],
                },
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
            activity_payloads=[
                {
                    "activity_type": RewardsActivityType.REWARD_STATUS,
                    "payload_formatter_fn": RewardsActivityType.get_reward_status_activity_data,
                    "formatter_kwargs": [
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "count": 2,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                        },
                        {
                            "account_holder_uuid": canned_account_holder_uuid,
                            "activity_datetime": now,
                            "activity_identifier": mock.ANY,  # pending reward uuid - a bit tricky to get
                            "campaigns": ["test-campaign"],
                            "count": 1,
                            "new_status": "deleted",
                            "original_status": "pending",
                            "reason": "Pending Reward removed due to refund",
                            "retailer_slug": "re-test",
                            "summary": "Test Retailer Pending reward deleted for test-campaign",
                        },
                    ],
                },
            ],
        ),
        id="refund, 2 PRRs w/o slush, balance < refund, very large refund -> 1 PRR removed & 1 PRR count decreased",
    ),
)
