import logging
import sys

from enum import Enum
from typing import TYPE_CHECKING

import typer

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from .generator import generate_account_holders_and_rewards, generate_retailer_base_config

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

app = typer.Typer()
logger = logging.getLogger(__name__)


class LoyaltyTypes(Enum):
    STAMPS = "STAMPS"
    ACCUMULATOR = "ACCUMULATOR"
    BOTH = "BOTH"


@app.command()
def populate(
    account_holders_to_create: int = typer.Option(
        10,
        "-n",
        help="number of account holders of each type to create",
    ),
    retailer: str = typer.Option(
        "test-retailer",
        "--retailer",
        "-r",
        prompt=True,
        help="retailer used for generated account holders",
    ),
    max_val: int = typer.Option(
        100, "--max-val", prompt=True, help="maximum balance value, decimals will be added at random"
    ),
    campaign: str = typer.Option(
        "test-campaign-1",
        "--campaign",
        "-c",
        help="backup campaign name used for generating balances if no active campaign is found",
    ),
    reward_slug: str = typer.Option("10percentoff", "--reward_slug", "-rs", help="Reward slug used for reward_config"),
    loyalty_type: str = typer.Option(
        LoyaltyTypes.ACCUMULATOR.value,
        "--loyalty-type",
        "-L",
        help="Select campaign loyalty type",
    ),
    db_host: str = typer.Option(
        "localhost",
        "--host",
        help="database host",
    ),
    db_port: str = typer.Option(
        "5432",
        "--port",
        help="database port",
    ),
    db_user: str = typer.Option(
        "postgres",
        "--user",
        help="database user",
    ),
    db_pass: str = typer.Option(
        "",
        "--password",
        help="database password",
    ),
    db_name: str = typer.Option(
        "cosmos",
        "--name",
        help="database name",
    ),
    unallocated_rewards_to_create: int = typer.Option(
        10,
        "--unallocated-rewards",
        prompt=True,
        help="total number of unallocated rewards to create",
    ),
    setup_retailer: bool = typer.Option(
        False,
        "--bootstrap-new-retailer",
        "-B",
        help="Sets up retailer, campaign, and reward config in addition to the usual account holders and rewards",
    ),
    refund_window: int = typer.Option(
        0,
        "--refund-window",
        help="Sets a refund window for reward rule. If reward goal is reached, pending rewards are created",
    ),
    fetch_type: str = typer.Option(
        "PRE_LOADED",
        "--fetch-type",
        help="Sets a fetch type for rewards that are generated. There are only a select few types",
    ),
    tx_history: bool = typer.Option(
        True,
        "--tx-history",
        "-T",
        help="Sets up transaction history for account holders",
    ),
    add_fetch_types: bool = typer.Option(
        False,
        "--add-fetch-types",
        "-F",
        help="Add PRE_LOADED and JIGSAW_EGIFT fetch types",
    ),
    sql_debug: bool = typer.Option(
        False,
        "--sql-debug",
        "-D",
        help="Add PRE_LOADED and JIGSAW_EGIFT fetch types",
    ),
) -> None:

    if max_val < 0:
        typer.echo("maximum balance value must be an integer greater than 1.")
        sys.exit(-1)

    if not 1000000000 > account_holders_to_create > 0:
        typer.echo("the number of account holders to create must be between 1 and 1,000,000,000.")
        sys.exit(-1)

    db_uri = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_uri, poolclass=NullPool, echo=sql_debug)

    db_session: "Session" = scoped_session(sessionmaker(bind=engine))()
    try:
        if setup_retailer:
            generate_retailer_base_config(
                db_session,
                retailer,
                campaign,
                reward_slug,
                refund_window,
                fetch_type,
                loyalty_type,
                add_fetch_types,
            )

        generate_account_holders_and_rewards(
            db_session,
            account_holders_to_create,
            retailer,
            campaign,
            max_val,
            unallocated_rewards_to_create,
            refund_window,
            tx_history,
            loyalty_type,
        )
    finally:
        db_session.close()

    typer.echo("\naccount holders and rewards created.")
    sys.exit(0)


if __name__ == "__main__":
    app()
