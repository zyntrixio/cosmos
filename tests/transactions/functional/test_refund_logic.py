from collections.abc import Callable
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from pytest_mock import MockerFixture
from sqlalchemy.future import select
from sqlalchemy.orm import joinedload

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.db.models import AccountHolder, Campaign, CampaignBalance, PendingReward, Transaction
from cosmos.transactions.api.service import TransactionService

from .fixtures.refund_logic import (
    ExpectationData,
    SetupData,
    canned_account_holder_uuid,
    canned_transaction_id,
    now,
    test_refund_data,
)

if TYPE_CHECKING:

    from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture(scope="function", name="balance_object")
async def balance_obj(
    async_db_session: "AsyncSession", account_holder: "AccountHolder", campaign_with_rules: Campaign
) -> CampaignBalance:
    balance_object = CampaignBalance(
        account_holder_id=account_holder.id,
        campaign_id=campaign_with_rules.id,
        balance=0,
    )
    async_db_session.add(balance_object)
    await async_db_session.commit()

    return (
        await async_db_session.execute(
            select(CampaignBalance)
            .options(
                joinedload(CampaignBalance.campaign).options(
                    joinedload(Campaign.earn_rule),
                    joinedload(Campaign.reward_rule),
                )
            )
            .where(CampaignBalance.id == balance_object.id)
        )
    ).scalar_one()


@pytest_asyncio.fixture(scope="function")
async def setup_data(
    request: pytest.FixtureRequest,
    async_db_session: "AsyncSession",
    account_holder: "AccountHolder",
    balance_object: CampaignBalance,
    create_pending_reward: Callable,
) -> None:
    for pr_data in request.param.pending_rewards:
        create_pending_reward(
            account_holder_id=account_holder.id,
            campaign_id=balance_object.campaign_id,
            created_date=pr_data.created_date,
            conversion_date=pr_data.conversion_date,
            count=pr_data.count,
            value=pr_data.value,
            total_cost_to_user=pr_data.total_cost_to_user,
            pending_reward_uuid=pr_data.pending_reward_uuid,
        )
    balance_object.balance = request.param.balance
    await async_db_session.commit()
    return request.param


@pytest.mark.asyncio
@pytest.mark.parametrize(("setup_data", "expectation_data"), test_refund_data, indirect=["setup_data"])
async def test__process_refund(
    mocker: MockerFixture,
    async_db_session: "AsyncSession",
    account_holder: AccountHolder,
    balance_object: CampaignBalance,
    setup_data: SetupData,
    expectation_data: ExpectationData,
) -> None:

    mock_datetime = mocker.patch("cosmos.core.activity.enums.datetime")
    mock_datetime.now.return_value = now

    account_holder.status = AccountHolderStatuses.ACTIVE
    account_holder.account_holder_uuid = canned_account_holder_uuid
    balance_object.campaign.reward_rule.allocation_window = 10
    transaction = Transaction(
        account_holder_id=account_holder.id,
        retailer_id=account_holder.retailer_id,
        transaction_id=str(canned_transaction_id),
        amount=setup_data.adjustment,
        mid="TSTMID",
        datetime=now,
        payment_transaction_id=f"processed:{canned_transaction_id}",
        processed=True,
    )
    async_db_session.add(transaction)
    await async_db_session.commit()

    mocked_store_activity = mocker.patch.object(TransactionService, "store_activity")
    service = TransactionService(async_db_session, account_holder.retailer)

    await service._adjust_balance(
        campaign=balance_object.campaign,
        campaign_balance=balance_object,
        transaction=transaction,
        account_holder_uuid=account_holder.account_holder_uuid,
    )
    await service.commit_db_changes()

    await async_db_session.refresh(balance_object)
    pending_rewards = (
        (
            await async_db_session.execute(
                select(PendingReward).where(
                    PendingReward.account_holder_id == account_holder.id,
                    PendingReward.campaign_id == balance_object.campaign_id,
                )
                # note this is the order in which they are added to the db so we can
                # use zip to assert on ordering in test_data
                .order_by(PendingReward.created_at)
            )
        )
        .scalars()
        .all()
    )
    assert len(pending_rewards) == len(expectation_data.pending_rewards)
    for (pending_reward, expected_pending_reward_data) in zip(
        pending_rewards, expectation_data.pending_rewards, strict=True
    ):
        assert pending_reward.count == expected_pending_reward_data.count
        assert pending_reward.value == expected_pending_reward_data.value
        assert pending_reward.total_value == expected_pending_reward_data.value * expected_pending_reward_data.count
        assert pending_reward.total_cost_to_user == expected_pending_reward_data.total_cost_to_user
        assert pending_reward.created_date == expected_pending_reward_data.created_date
        assert pending_reward.conversion_date == expected_pending_reward_data.conversion_date

    assert balance_object.balance == expectation_data.balance

    def _decent_error_message() -> str:
        activities = [call.kwargs["activity_type"].name for call in mocked_store_activity.mock_calls]
        expected = [expected_activity.name for expected_activity, _ in expectation_data.activities]
        return f"Expected: {expected}, got instead: {activities}"

    assert mocked_store_activity.call_count == len(expectation_data.activities), _decent_error_message()

    for call, (expected_activity_type, payload_count) in zip(
        mocked_store_activity.mock_calls, expectation_data.activities, strict=True
    ):
        assert call.kwargs["activity_type"] == expected_activity_type
        formatter_kwargs = call.kwargs["formatter_kwargs"]
        if isinstance(formatter_kwargs, dict):
            assert payload_count == 1, f"formatted_kwargs len = 1, expected {payload_count}"
        else:
            assert len(formatter_kwargs) == payload_count

    if expectation_data.activity_payloads:
        for call, payload in zip(mocked_store_activity.mock_calls, expectation_data.activity_payloads, strict=True):
            assert call.kwargs == payload
