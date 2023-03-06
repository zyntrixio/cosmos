from collections.abc import Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, NamedTuple, cast
from uuid import UUID

from sqlalchemy import Table, or_

from cosmos.db.base_class import async_run_query
from cosmos.db.models import AccountHolder, PendingReward, Reward

from . import logger

if TYPE_CHECKING:  # pragma: no cover

    from sqlalchemy.engine import RowMapping
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.ext.asyncio.session import AsyncSessionTransaction

    from cosmos.db.models import Campaign


class PendingRewardsRes(NamedTuple):
    pending_reward_id: int
    pending_reward_uuid: UUID
    pending_reward_count: int
    account_holder_id: int
    account_holder_uuid: UUID


class CancelIssuedRewardsRes(NamedTuple):
    cancelled_date: datetime
    reward_uuid: UUID
    account_holder_uuid: UUID


async def delete_pending_rewards_for_campaign(
    db_session: "AsyncSession", *, campaign: "Campaign"
) -> list["PendingRewardsRes"]:
    logger.info("Deleting pending rewards for campaign '%s'...", campaign.slug)

    async def _delete_pending_rewards(
        savepoint: "AsyncSessionTransaction",
    ) -> Sequence["RowMapping"]:
        result = (
            await db_session.execute(
                cast(Table, PendingReward.__table__)
                .delete()
                .where(
                    PendingReward.campaign_id == campaign.id,
                    PendingReward.account_holder_id == AccountHolder.id,
                    AccountHolder.retailer_id == campaign.retailer_id,
                )
                .returning(
                    PendingReward.id.label("pending_reward_id"),
                    PendingReward.pending_reward_uuid,
                    PendingReward.count.label("pending_reward_count"),
                    PendingReward.account_holder_id,
                    AccountHolder.account_holder_uuid,
                )
            )
        ).mappings()

        await savepoint.commit()
        return result.all()

    del_data = await async_run_query(_delete_pending_rewards, db_session)
    logger.info(f"Deleted {len(del_data)} pending rewards")
    return [PendingRewardsRes(**res) for res in del_data]


async def cancel_issued_rewards_for_campaign(
    db_session: "AsyncSession", *, campaign: "Campaign"
) -> list["CancelIssuedRewardsRes"]:
    # asyncpg can't translate tz aware to naive datetimes, and db datetimes are naive.
    now = datetime.now(tz=UTC).replace(tzinfo=None)

    async def _query(savepoint: "AsyncSessionTransaction") -> Sequence["RowMapping"]:
        updates = (
            await db_session.execute(
                cast(Table, Reward.__table__)
                .update()
                .values(cancelled_date=now)
                .where(
                    Reward.account_holder_id.is_not(None),
                    Reward.issued_date.is_not(None),
                    or_(Reward.expiry_date.is_(None), Reward.expiry_date >= now),
                    Reward.campaign_id == campaign.id,
                    Reward.deleted.is_(False),
                    # to get account holder uuid
                    Reward.account_holder_id == AccountHolder.id,
                )
                .returning(Reward.cancelled_date, Reward.reward_uuid, AccountHolder.account_holder_uuid)
            )
        ).mappings()

        await savepoint.commit()
        return updates.all()

    updates = await async_run_query(_query, db_session)
    logger.info(f"Cancelled {len(updates)} rewards")
    return [CancelIssuedRewardsRes(**u) for u in updates]


async def transfer_pending_rewards(
    db_session: "AsyncSession", *, from_campaign: "Campaign", to_campaign: "Campaign"
) -> list["PendingRewardsRes"]:
    async def _query(savepoint: "AsyncSessionTransaction") -> Sequence["RowMapping"]:
        updates = (
            await db_session.execute(
                cast(Table, PendingReward.__table__)
                .update()
                .values(campaign_id=to_campaign.id)
                .where(
                    PendingReward.campaign_id == from_campaign.id,
                    # this is needed to return the account_holder_uuid
                    PendingReward.account_holder_id == AccountHolder.id,
                )
                .returning(
                    PendingReward.id.label("pending_reward_id"),
                    PendingReward.pending_reward_uuid,
                    PendingReward.count.label("pending_reward_count"),
                    PendingReward.account_holder_id,
                    AccountHolder.account_holder_uuid,
                )
            )
        ).mappings()
        await savepoint.commit()
        return updates.all()

    updates = await async_run_query(_query, db_session)
    logger.info(f"Transferred {len(updates)} rewards")
    return [PendingRewardsRes(**u) for u in updates]
