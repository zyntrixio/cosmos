from typing import TYPE_CHECKING
from uuid import uuid4

import typer

from progressbar import ProgressBar

from .crud import (
    batch_create_account_holders_and_rewards,
    clear_existing_account_holders,
    create_unallocated_rewards,
    delete_insert_fetch_types,
    get_active_campaigns,
    get_retailer_by_slug,
    get_reward_config_and_retailer,
    get_reward_rule,
    setup_retailer,
)
from .enums import AccountHolderTypes

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

BATCH_SIZE = 1000


def _generate_account_holders_and_rewards_data(
    db_session: "Session",
    ah_to_create: int,
    retailer_slug: str,
    campaign_slug: str,
    max_val: int,
    unallocated_rewards_to_create: int,
    refund_window: int | None,
    tx_history: bool,
) -> None:

    retailer = get_retailer_by_slug(db_session, retailer_slug)
    typer.echo(f"Selected retailer: {retailer.name}")
    reward_config, retailer = get_reward_config_and_retailer(db_session, retailer_slug)
    typer.echo(f"Reward slug for {retailer.name}: {reward_config.id}")
    active_campaigns = get_active_campaigns(db_session, retailer)
    typer.echo(f"Selected campaign {campaign_slug}.")
    reward_rule = get_reward_rule(db_session, campaign_slug)
    typer.echo("Deleting previously generated account holders for requested retailer.")
    clear_existing_account_holders(db_session, retailer.id)
    for campaign in active_campaigns:
        unallocated_rewards_batch = create_unallocated_rewards(
            unallocated_rewards_to_create=unallocated_rewards_to_create,
            batch_reward_salt=str(uuid4()),
            campaign=campaign,
            reward_config=reward_config,
        )
        db_session.bulk_save_objects(unallocated_rewards_batch)
        db_session.commit()

    for account_holder_type in AccountHolderTypes:
        typer.echo("\ncreating %s users." % account_holder_type.value)
        batch_start = ah_to_create
        progress_counter = 0

        with ProgressBar(max_value=ah_to_create) as progress_bar:
            while batch_start > 0:

                batch_end = 0 if batch_start <= BATCH_SIZE else batch_start - BATCH_SIZE
                progress_counter = batch_create_account_holders_and_rewards(
                    db_session=db_session,
                    batch_start=batch_start,
                    batch_end=batch_end,
                    account_holder_type=account_holder_type,
                    retailer_config=retailer,
                    active_campaigns=active_campaigns,
                    max_val=max_val,
                    bar=progress_bar,
                    progress_counter=progress_counter,
                    account_holder_type_reward_code_salt=str(uuid4()),
                    refund_window=refund_window,
                    tx_history=tx_history,
                    reward_goal=reward_rule.reward_goal,
                    reward_config=reward_config,
                )
                batch_start = batch_end


def generate_account_holders_and_rewards(
    db_session: "Session",
    ah_to_create: int,
    retailer_slug: str,
    campaign_slug: str,
    max_val: int,
    unallocated_rewards_to_create: int,
    refund_window: int,
    tx_history: bool,
    loyalty_type: str,
) -> None:
    if loyalty_type == "BOTH":
        for loyalty in ["ACCUMULATOR", "STAMPS"]:
            loyalty_retailer_slug = f"{retailer_slug}-{loyalty}"
            loyalty_campaign_slug = f"{campaign_slug}-{loyalty}"
            _generate_account_holders_and_rewards_data(
                db_session,
                ah_to_create,
                loyalty_retailer_slug,
                loyalty_campaign_slug,
                max_val,
                unallocated_rewards_to_create,
                refund_window,
                tx_history,
            )
    else:
        _generate_account_holders_and_rewards_data(
            db_session,
            ah_to_create,
            retailer_slug,
            campaign_slug,
            max_val,
            unallocated_rewards_to_create,
            refund_window,
            tx_history,
        )


def generate_retailer_base_config(
    db_session: "Session",
    retailer_slug: str,
    campaign_slug: str,
    reward_slug: str,
    refund_window: int,
    fetch_type: str,
    loyalty_type: str,
    add_fetch_types: bool,
) -> None:
    if add_fetch_types:
        typer.echo("Creating fetch types...")
        delete_insert_fetch_types(db_session)
    typer.echo(f"Creating '{retailer_slug}' retailer.")
    if loyalty_type == "BOTH":
        for loyalty in ["ACCUMULATOR", "STAMPS"]:
            loyalty_retailer_slug = f"{retailer_slug}-{loyalty.lower()}"
            loyalty_campaign_slug = f"{campaign_slug}-{loyalty.lower()}"
            setup_retailer(
                db_session,
                retailer_slug=loyalty_retailer_slug,
                campaign_slug=loyalty_campaign_slug,
                fetch_type_name=fetch_type,
                loyalty_type=loyalty,
                refund_window=refund_window,
                reward_slug=reward_slug,
            )
    else:
        setup_retailer(
            db_session,
            retailer_slug=retailer_slug,
            fetch_type_name=fetch_type,
            campaign_slug=campaign_slug,
            refund_window=refund_window,
            loyalty_type=loyalty_type,
            reward_slug=reward_slug,
        )
