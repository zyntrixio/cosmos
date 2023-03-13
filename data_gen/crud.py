import sys

from datetime import UTC, datetime, timedelta
from random import randint
from typing import TYPE_CHECKING
from uuid import uuid4

import click

from hashids import Hashids
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.future import select

from cosmos.db.models import (
    AccountHolder,
    AccountHolderProfile,
    Campaign,
    EarnRule,
    FetchType,
    MarketingPreference,
    PendingReward,
    Retailer,
    RetailerFetchType,
    RetailerStore,
    Reward,
    RewardConfig,
    RewardRule,
    Transaction,
    TransactionEarn,
)

from .enums import AccountHolderRewardStatuses, AccountHolderTypes, FetchTypesEnum
from .fixtures import (
    ACCOUNT_HOLDER_REWARD_SWITCHER,
    account_holder_payload,
    account_holder_pending_reward_payload,
    account_holder_profile_payload,
    account_holder_reward_payload,
    account_holder_transaction_payload,
    campaign_payload,
    earn_rule_payload,
    generate_tx_rows,
    marketing_preference_payload,
    retailer_data,
    retailer_fetch_type_payload,
    reward_config_payload,
    reward_rule_payload,
)
from .utils import generate_account_holder_campaign_balances

if TYPE_CHECKING:
    from progressbar import ProgressBar
    from sqlalchemy.orm import Session


def _get_retailer(db_session: "Session", retailer_slug: str) -> Retailer:
    return db_session.execute(select(Retailer).where(Retailer.slug == retailer_slug)).scalar()


def get_reward_config_and_retailer(db_session: "Session", retailer_slug: str) -> tuple[RewardConfig, Retailer]:
    retailer = _get_retailer(db_session, retailer_slug)
    reward_config = db_session.scalar(select(RewardConfig).where(RewardConfig.retailer_id == retailer.id))
    if not reward_config:
        click.echo(f"No reward config found for retailer: {retailer_slug}")
        sys.exit(-1)

    return reward_config, retailer


def create_unallocated_rewards(
    unallocated_rewards_to_create: int, batch_reward_salt: str, campaign: Campaign, reward_config: RewardConfig
) -> list[Reward]:
    hashids = Hashids(batch_reward_salt, min_length=15)
    unallocated_rewards = []
    now = datetime.now(tz=UTC)
    for i in range(unallocated_rewards_to_create):
        code = (hashids.encode(i),)
        unallocated_rewards.append(
            Reward(
                code=code,
                reward_config_id=reward_config.id,
                retailer_id=campaign.retailer_id,
                deleted=False,
                issued_date=now,
                expiry_date=now + timedelta(days=30),
                campaign_id=campaign.id,
            )
        )

    return unallocated_rewards


def _get_fetch_type(db_session: "Session", fetch_type_name: str) -> FetchType:
    if not hasattr(FetchTypesEnum, fetch_type_name):
        raise ValueError(f"Unknown fetch type {fetch_type_name}")
    return db_session.execute(select(FetchType).where(FetchType.name == fetch_type_name)).scalar()


def get_retailer_by_slug(db_session: "Session", retailer_slug: str) -> Retailer:
    retailer = db_session.scalar(select(Retailer).where(Retailer.slug == retailer_slug))
    if not retailer:
        click.echo(f"requested retailer '{retailer_slug}' does not exists in DB.")
        sys.exit(-1)

    return retailer


def batch_create_account_holders_and_rewards(
    *,
    db_session: "Session",
    batch_start: int,
    batch_end: int,
    account_holder_type: AccountHolderTypes,
    retailer_config: Retailer,
    active_campaigns: list[Campaign],
    max_val: int,
    bar: "ProgressBar",
    progress_counter: int,
    account_holder_type_reward_code_salt: str,
    refund_window: int | None,
    tx_history: bool,
    reward_goal: int,
    reward_config: RewardConfig,
) -> int:
    if refund_window is None:
        refund_window = 0
    account_holders_batch = []
    account_holders_profile_batch = []
    account_holders_marketing_batch = []
    account_holder_balance_batch = []

    account_holder_rewards_batch = []
    account_holder_transaction_history_batch = []
    batch_range = range(batch_start, batch_end, -1)

    account_holders_batch = [
        AccountHolder(**account_holder_payload(i, account_holder_type, retailer_config)) for i in batch_range
    ]
    db_session.add_all(account_holders_batch)
    db_session.flush()

    for account_holder, i in zip(account_holders_batch, batch_range):
        if tx_history:
            transactions = _generate_account_holder_transaction_history(
                db_session,
                account_holder,
                retailer_config,
                reward_goal,
                active_campaigns,
            )
            account_holder_transaction_history_batch.extend(transactions)
        account_holder_balance_batch.extend(
            generate_account_holder_campaign_balances(account_holder, active_campaigns, account_holder_type, max_val)
        )
        account_holders_profile_batch.append(AccountHolderProfile(**account_holder_profile_payload(account_holder)))
        account_holders_marketing_batch.append(MarketingPreference(**marketing_preference_payload(account_holder)))
        account_holder_rewards = _generate_allocated_rewards(
            i,
            account_holder,
            account_holder_type_reward_code_salt,
            active_campaigns,
            reward_config,
        )
        account_holder_rewards_batch.extend(account_holder_rewards)
        if refund_window > 0:
            account_holder_pending_rewards = _generate_account_holder_pending_rewards(
                i, account_holder, active_campaigns, refund_window
            )
            db_session.bulk_save_objects(account_holder_pending_rewards)
        progress_counter += 1
        bar.update(progress_counter)

    db_session.bulk_save_objects(account_holder_transaction_history_batch)
    db_session.bulk_save_objects(account_holders_profile_batch)
    db_session.bulk_save_objects(account_holders_marketing_batch)
    db_session.bulk_save_objects(account_holder_rewards_batch)
    db_session.bulk_save_objects(account_holder_balance_batch)

    return progress_counter


def _generate_allocated_rewards(
    account_holder_n: int | str,
    account_holder: AccountHolder,
    batch_reward_salt: str,
    active_campaigns: list[Campaign],
    reward_config: RewardConfig,
) -> list[Reward]:
    hashids = Hashids(batch_reward_salt, min_length=15)

    def _generate_rewards(rewards_required: list[tuple[int, AccountHolderRewardStatuses]]) -> list[Reward]:
        account_holder_rewards: list[Reward] = []
        for i, (how_many, reward_status) in enumerate(rewards_required):
            for campaign in active_campaigns:
                if reward_status == AccountHolderRewardStatuses.PENDING:
                    continue
                for reward_n in range(how_many):
                    reward_uuid = uuid4()
                    reward_code = hashids.encode(i, reward_n, account_holder_n)
                    account_holder_rewards.append(
                        Reward(
                            **account_holder_reward_payload(
                                account_holder_id=account_holder.id,
                                retailer_id=account_holder.retailer.id,
                                campaign_id=campaign.id,
                                reward_uuid=reward_uuid,
                                reward_code=reward_code,
                                reward_config_id=reward_config.id,
                                reward_status=reward_status,
                            )
                        )
                    )

        return account_holder_rewards

    account_holder_reward_type = int(account_holder_n) % 11
    return _generate_rewards(ACCOUNT_HOLDER_REWARD_SWITCHER[account_holder_reward_type])


def _generate_account_holder_pending_rewards(
    account_holder_n: int | str,
    account_holder: AccountHolder,
    active_campaigns: list[Campaign],
    refund_window: int,
) -> list[PendingReward]:
    def _generate_pending_rewards(
        pending_rewards_required: list[tuple[int, AccountHolderRewardStatuses]]
    ) -> list[PendingReward]:
        account_holder_pending_rewards: list[PendingReward] = []
        for campaign in active_campaigns:
            for _, (how_many, reward_status) in enumerate(pending_rewards_required):
                if reward_status != AccountHolderRewardStatuses.PENDING:
                    continue
                for _ in range(how_many):
                    account_holder_pending_rewards.append(
                        PendingReward(
                            **account_holder_pending_reward_payload(
                                account_holder_id=account_holder.id,
                                campaign_id=campaign.id,
                                refund_window=refund_window,
                            )
                        )
                    )

        return account_holder_pending_rewards

    account_holder_reward_type = int(account_holder_n) % 11
    return _generate_pending_rewards(ACCOUNT_HOLDER_REWARD_SWITCHER[account_holder_reward_type])


def _generate_account_holder_transaction_history(
    db_session: "Session",
    account_holder: AccountHolder,
    retailer: Retailer,
    reward_goal: int,
    active_campaigns: list[Campaign],
) -> list[Transaction | TransactionEarn]:
    account_holder_transaction_history: list[Transaction] = []
    how_many = randint(1, 10)
    tx_history_rows = generate_tx_rows(reward_goal, retailer=retailer)
    for tx_history in tx_history_rows[:how_many]:
        transaction = Transaction(
            **account_holder_transaction_payload(
                retailer_id=retailer.id,
                account_holder_id=account_holder.id,
                tx_amount=tx_history.tx_amount,
                mid=tx_history.mid,
            )
        )
        db_session.add(transaction)
        db_session.flush()
        account_holder_transaction_history.append(transaction)
        for campaign in active_campaigns:
            if campaign.loyalty_type.name == "STAMPS":
                earn_amount = 0 if float(tx_history.tx_amount) <= 0 else 1
            else:
                earn_amount = tx_history.tx_amount
            account_holder_transaction_history.append(
                TransactionEarn(
                    transaction_id=transaction.id,
                    loyalty_type=campaign.loyalty_type,
                    earn_amount=earn_amount,
                )
            )

    return account_holder_transaction_history


def clear_existing_account_holders(db_session: "Session", retailer_id: int) -> None:
    db_session.execute(
        delete(AccountHolder)
        .where(
            AccountHolder.retailer_id == retailer_id,
            AccountHolder.email.like(r"test_%_user_%@autogen.bpl"),
        )
        .execution_options(synchronize_session=False)
    )


def setup_retailer(
    db_session: "Session",
    *,
    retailer_slug: str,
    fetch_type_name: str,
    loyalty_type: str,
    campaign_slug: str,
    reward_slug: str,
    refund_window: int | None,
) -> Retailer:
    retailer = _get_retailer(db_session, retailer_slug)
    if retailer:
        db_session.execute(
            delete(Reward)
            .where(Reward.retailer_id == retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(AccountHolder)
            .where(AccountHolder.retailer_id == Retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(Campaign)
            .where(Campaign.retailer_id == retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(RetailerFetchType)
            .where(RetailerFetchType.retailer_id == retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(RewardConfig)
            .where(RewardConfig.retailer_id == retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.execute(
            delete(RetailerStore)
            .where(RetailerStore.retailer_id == retailer.id, Retailer.slug == retailer_slug)
            .execution_options(synchronize_session=False)
        )
        db_session.delete(retailer)

    fetch_type = _get_fetch_type(db_session, fetch_type_name)
    retailer = Retailer(**retailer_data(retailer_slug))
    db_session.add(retailer)
    db_session.flush()
    for i in range(1, 6):
        db_session.add(RetailerStore(store_name=f"Super Store {i}", mid=f"mid-{i}", retailer_id=retailer.id))
    db_session.add(RetailerFetchType(**retailer_fetch_type_payload(retailer.id, fetch_type.id)))
    reward_config = RewardConfig(**reward_config_payload(retailer.id, fetch_type.id, reward_slug))
    db_session.add(reward_config)
    db_session.flush()
    if loyalty_type == "STAMPS":
        refund_window = None
    campaign = Campaign(**campaign_payload(retailer.id, campaign_slug, loyalty_type))
    db_session.add(campaign)
    db_session.flush()
    db_session.add(RewardRule(**reward_rule_payload(campaign.id, reward_config.id, refund_window)))
    db_session.add(EarnRule(**earn_rule_payload(campaign.id, loyalty_type)))
    return retailer


def delete_insert_fetch_types(db_session: "Session") -> None:
    db_session.execute(
        insert(FetchType)
        .values(
            [
                {
                    "name": "PRE_LOADED",
                    "required_fields": "validity_days: integer",
                    "path": "carina.fetch_reward.pre_loaded.PreLoaded",
                },
                {
                    "name": "JIGSAW_EGIFT",
                    "required_fields": "transaction_value: integer",
                    "path": "carina.fetch_reward.jigsaw.Jigsaw",
                },
            ]
        )
        .on_conflict_do_nothing()
    )


def get_active_campaigns(db_session: "Session", retailer: Retailer) -> list[Campaign]:
    return (
        db_session.execute(
            select(Campaign).where(
                Campaign.status == "ACTIVE",
                Campaign.retailer_id == Retailer.id,
                Retailer.slug == retailer.slug,
            )
        )
        .scalars()
        .all()
    )


def get_campaign(db_session: "Session", campaign_slug: str) -> Campaign:
    return db_session.execute(
        select(Campaign).where(
            Campaign.slug == campaign_slug,
        )
    ).scalar_one()


def get_reward_rule(db_session: "Session", campaign_slug: str) -> RewardRule:
    campaign = get_campaign(db_session, campaign_slug)
    return db_session.execute(
        select(RewardRule).where(
            RewardRule.campaign_id == campaign.id,
        )
    ).scalar_one()
