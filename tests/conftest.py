from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Generator, NamedTuple
from uuid import uuid4

import pytest

from pytest_mock import MockerFixture
from sqlalchemy_utils import create_database, database_exists, drop_database
from testfixtures import LogCapture

from cosmos.accounts.enums import AccountHolderStatuses
from cosmos.campaigns.enums import LoyaltyTypes
from cosmos.core.config import redis
from cosmos.db.base import Base
from cosmos.db.models import (
    AccountHolder,
    AccountHolderProfile,
    Campaign,
    CampaignBalance,
    EarnRule,
    FetchType,
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
from cosmos.db.session import SyncSessionMaker, sync_engine
from cosmos.retailers.enums import RetailerStatuses

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from sqlalchemy.orm import Session


class SetupType(NamedTuple):
    db_session: "Session"
    retailer: Retailer
    account_holder: AccountHolder


@pytest.fixture(scope="session", autouse=True)
def setup_db() -> Generator:
    if sync_engine.url.database != "cosmos_test":
        raise ValueError(f"Unsafe attempt to recreate database: {sync_engine.url.database}")

    if database_exists(sync_engine.url):
        drop_database(sync_engine.url)

    create_database(sync_engine.url)

    yield

    # At end of all tests, drop the test db
    drop_database(sync_engine.url)


@pytest.fixture(scope="function", autouse=True)
def setup_tables() -> Generator:
    """
    autouse set to True so will be run before each test function, to set up tables
    and tear them down after each test runs
    """

    Base.metadata.create_all(bind=sync_engine)

    yield

    # Drop all tables after each test
    Base.metadata.drop_all(bind=sync_engine)


@pytest.fixture(scope="session")
def main_db_session() -> Generator["Session", None, None]:
    with SyncSessionMaker() as session:
        yield session


@pytest.fixture(scope="function")
def db_session(main_db_session: "Session") -> Generator["Session", None, None]:
    yield main_db_session
    main_db_session.rollback()
    main_db_session.expunge_all()


@pytest.fixture(scope="session", autouse=True)
def setup_redis() -> Generator:

    yield

    # At end of all tests, delete the tasks from the queue
    redis.flushdb()


@pytest.fixture(scope="function")
def mock_activity(mocker: MockerFixture) -> "MagicMock":
    return mocker.patch("cosmos.core.api.service.format_and_send_activity_in_background")


@pytest.fixture(scope="function")
def log_capture() -> Generator:
    with LogCapture() as cpt:
        yield cpt


@pytest.fixture(scope="function")
def test_retailer() -> dict:
    return {
        "name": "Test Retailer",
        "slug": "re-test",
        "status": RetailerStatuses.TEST,
        "account_number_prefix": "RTST",
        "profile_config": (
            "email:"
            "\n  required: true"
            "\nfirst_name:"
            "\n  required: true"
            "\nlast_name:"
            "\n  required: true"
            "\ndate_of_birth:"
            "\n  required: true"
            "\nphone:"
            "\n  required: true"
            "\naddress_line1:"
            "\n  required: true"
            "\naddress_line2:"
            "\n  required: true"
            "\npostcode:"
            "\n  required: true"
            "\ncity:"
            "\n  required: true"
        ),
        "marketing_preference_config": "marketing_pref:\n  type: boolean\n  label: Sample Question?",
        "loyalty_name": "Test Retailer",
    }


@pytest.fixture(scope="function")
def retailer(db_session: "Session", test_retailer: dict) -> Retailer:
    retailer = Retailer(**test_retailer)
    db_session.add(retailer)
    db_session.commit()

    return retailer


@pytest.fixture(scope="function")
def create_retailer(
    db_session: "Session",
    test_retailer: dict,
) -> Callable[..., Retailer]:
    def _create_retailer(**params: str | int) -> Retailer:
        test_retailer.update(params)
        retailer = Retailer(**test_retailer)
        db_session.add(retailer)
        db_session.commit()

        return retailer

    return _create_retailer


@pytest.fixture(scope="function")
def test_account_holder_activation_data() -> dict:
    return {
        "email": "activate_1@test.user",
        "credentials": {
            "first_name": "Test User",
            "last_name": "Test 1",
            "date_of_birth": datetime.strptime("1970-12-03", "%Y-%m-%d").replace(tzinfo=timezone.utc).date(),
            "phone": "+447968100999",
            "address_line1": "Flat 3, Some Place",
            "address_line2": "Some Street",
            "postcode": "BN77CC",
            "city": "Brighton & Hove",
        },
    }


@pytest.fixture(scope="function")
def account_holder(
    db_session: "Session",
    retailer: Retailer,
    test_account_holder_activation_data: dict,
) -> AccountHolder:
    acc_holder = AccountHolder(
        email=test_account_holder_activation_data["email"],
        account_number=None,
        retailer_id=retailer.id,
        status=AccountHolderStatuses.PENDING,
    )
    db_session.add(acc_holder)
    db_session.flush()

    profile = AccountHolderProfile(
        account_holder_id=acc_holder.id, **test_account_holder_activation_data["credentials"]
    )
    db_session.add(profile)
    db_session.commit()

    return acc_holder


@pytest.fixture(scope="function")
def create_account_holder(
    db_session: "Session", retailer: Retailer, test_account_holder_activation_data: dict
) -> Callable[..., AccountHolder]:

    data = {
        "email": test_account_holder_activation_data["email"],
        "retailer_id": retailer.id,
        "status": "ACTIVE",
    }

    def _create_account_holder(**params: str | int) -> AccountHolder:
        data.update(params)
        acc_holder = AccountHolder(**data)
        db_session.add(acc_holder)
        db_session.flush()

        profile = AccountHolderProfile(
            account_holder_id=acc_holder.id, **test_account_holder_activation_data["credentials"]
        )
        db_session.add(profile)
        db_session.commit()

        return acc_holder

    return _create_account_holder


@pytest.fixture(scope="function")
def setup(db_session: "Session", retailer: Retailer, account_holder: AccountHolder) -> Generator[SetupType, None, None]:
    yield SetupType(db_session, retailer, account_holder)


@pytest.fixture(scope="function")
def pre_loaded_fetch_type(db_session: "Session") -> FetchType:
    ft = FetchType(
        name="PRE_LOADED",
        required_fields="validity_days: integer",
        path="cosmos.rewards.fetch_reward.pre_loaded.PreLoaded",
    )
    db_session.add(ft)
    db_session.commit()
    return ft


@pytest.fixture(scope="function")
def jigsaw_fetch_type(db_session: "Session") -> FetchType:
    ft = FetchType(
        name="JIGSAW_EGIFT",
        path="cosmos.rewards.fetch_reward.jigsaw.Jigsaw",
        required_fields="transaction_value: integer",
    )
    db_session.add(ft)
    db_session.commit()
    return ft


@pytest.fixture(scope="function")
def reward_config(setup: SetupType, pre_loaded_fetch_type: FetchType) -> RewardConfig:
    db_session, retailer, _ = setup
    mock_reward_config = RewardConfig(
        slug="test-reward-slug",
        required_fields_values="validity_days: 15",
        retailer_id=retailer.id,
        fetch_type_id=pre_loaded_fetch_type.id,
        active=True,
    )
    db_session.add(mock_reward_config)
    db_session.commit()
    return mock_reward_config


@pytest.fixture()
def mock_campaign_balance_data() -> list[dict]:
    return [
        {"value": 0.0, "campaign_slug": "test-campaign"},
    ]


@pytest.fixture(scope="function")
def campaigns(setup: SetupType, mock_campaign_balance_data: dict) -> list[Campaign]:
    db_session, retailer, _ = setup
    campaigns = []
    for balance_data in mock_campaign_balance_data:
        mock_campaign = Campaign(
            status="ACTIVE",
            name=balance_data["campaign_slug"],
            slug=balance_data["campaign_slug"],
            retailer_id=retailer.id,
            loyalty_type="ACCUMULATOR",
        )
        db_session.add(mock_campaign)
        campaigns.append(mock_campaign)
    db_session.commit()
    return campaigns


@pytest.fixture(scope="function")
def campaign(campaigns: list[Campaign]) -> Campaign:
    return campaigns[0]


@pytest.fixture(scope="function")
def create_campaign(setup: SetupType, reward_config: RewardConfig) -> Callable[..., Campaign]:
    db_session, retailer, _ = setup
    data = {
        "status": "ACTIVE",
        "name": "test campaign",
        "slug": "test-campaign",
        "retailer_id": retailer.id,
        "loyalty_type": "ACCUMULATOR",
    }

    def _create_campaign(**params: Any) -> Campaign:  # noqa: ANN401
        """
        Create a campaign in the test DB
        :param params: override any values for campaign
        :return: Campaign
        """
        data.update(params)
        new_campaign = Campaign(**data)

        db_session.add(new_campaign)
        db_session.flush()
        db_session.add(
            RewardRule(
                reward_goal=500,
                campaign_id=new_campaign.id,
                reward_config_id=reward_config.id,
            )
        )
        db_session.add(
            EarnRule(
                threshold=100,
                increment=1,
                campaign_id=new_campaign.id,
            )
        )
        db_session.commit()
        db_session.refresh(new_campaign)

        return new_campaign

    return _create_campaign


@pytest.fixture(scope="function")
def campaign_with_rules(setup: SetupType, campaign: Campaign, reward_config: RewardConfig) -> Campaign:
    db_session = setup.db_session
    campaign.start_date = datetime.now(timezone.utc) - timedelta(days=20)
    db_session.add(
        RewardRule(
            reward_goal=500,
            campaign_id=campaign.id,
            reward_config_id=reward_config.id,
        )
    )
    db_session.add(
        EarnRule(
            threshold=100,
            increment=1,
            campaign_id=campaign.id,
        )
    )
    db_session.commit()
    db_session.refresh(campaign)
    return campaign


@pytest.fixture(scope="function")
def account_holder_campaign_balances(setup: SetupType, campaigns: list[Campaign]) -> None:
    db_session, _, account_holder = setup
    for campaign in campaigns:
        db_session.add(
            CampaignBalance(
                account_holder_id=account_holder.id,
                campaign_id=campaign.id,
                balance=0,
            )
        )
    db_session.commit()


@pytest.fixture(scope="function")
def user_reward(setup: SetupType, reward_config: RewardConfig, campaign: Campaign) -> Reward:
    now = datetime.now(tz=timezone.utc)
    db_session, retailer, _ = setup
    mock_user_reward = Reward(
        reward_uuid=uuid4(),
        code="TSTCD123456",
        reward_config_id=reward_config.id,
        retailer_id=retailer.id,
        campaign_id=campaign.id,
        deleted=False,
        issued_date=now,
        expiry_date=now + timedelta(days=10),
    )
    db_session.add(mock_user_reward)
    db_session.commit()
    return mock_user_reward


@pytest.fixture(scope="function")
def create_mock_reward(db_session: "Session", reward_config: RewardConfig, campaign: Campaign) -> Callable:
    reward = {
        "reward_uuid": None,
        "account_holder_id": None,
        "reward_config_id": reward_config.id,
        "code": "test_reward_code",
        "deleted": False,
        "issued_date": datetime(2021, 6, 25, 14, 30, 00, tzinfo=timezone.utc),
        "expiry_date": datetime(2121, 6, 25, 14, 30, 00, tzinfo=timezone.utc),
        "redeemed_date": None,
        "cancelled_date": None,
        "retailer_id": None,
        "campaign_id": None,
        "created_at": datetime.now(tz=timezone.utc),
        "updated_at": None,
    }

    def _create_mock_reward(**reward_params: Any) -> Reward:  # noqa: ANN401
        """
        Create a reward in the test DB
        :param reward_params: override any values for reward
        :return: Callable function
        """
        reward.update(reward_params)
        mock_reward = Reward(**reward)

        db_session.add(mock_reward)
        db_session.commit()

        return mock_reward

    return _create_mock_reward


@pytest.fixture(scope="function")
def create_mock_account_holder(
    db_session: "Session",
    test_account_holder_activation_data: dict,
) -> Callable:
    def _create_mock_account_holder(
        retailer_id: int,
        **account_holder_params: dict,
    ) -> AccountHolder:
        test_account_holder_activation_data.update(account_holder_params)
        acc_holder = AccountHolder(email=test_account_holder_activation_data["email"], retailer_id=retailer_id)
        db_session.add(acc_holder)
        db_session.flush()

        profile = AccountHolderProfile(
            account_holder_id=acc_holder.id, **test_account_holder_activation_data["credentials"]
        )
        db_session.add(profile)
        db_session.commit()

        return acc_holder

    return _create_mock_account_holder


@pytest.fixture(scope="function")
def jigsaw_retailer_fetch_type(
    db_session: "Session", retailer: Retailer, jigsaw_fetch_type: FetchType
) -> RetailerFetchType:
    rft = RetailerFetchType(
        retailer_id=retailer.id,
        fetch_type_id=jigsaw_fetch_type.id,
        agent_config='base_url: "http://test.url"\n' "brand_id: 30\n" "fetch_reward: true\n" 'fetch_balance: false"',
    )
    db_session.add(rft)
    db_session.commit()
    return rft


@pytest.fixture(scope="function")
def pre_loaded_retailer_fetch_type(
    db_session: "Session", retailer: Retailer, pre_loaded_fetch_type: FetchType
) -> RetailerFetchType:
    rft = RetailerFetchType(
        retailer_id=retailer.id,
        fetch_type_id=pre_loaded_fetch_type.id,
    )
    db_session.add(rft)
    db_session.commit()
    return rft


@pytest.fixture(scope="function")
def create_reward_config(db_session: "Session", pre_loaded_retailer_fetch_type: RetailerFetchType) -> Callable:
    def _create_reward_config(**reward_config_params: Any) -> RewardConfig:  # noqa: ANN401
        mock_reward_config_params = {
            "slug": "test-reward",
            "required_fields_values": "validity_days: 15",
            "retailer_id": pre_loaded_retailer_fetch_type.retailer_id,
            "fetch_type_id": pre_loaded_retailer_fetch_type.fetch_type_id,
            "active": True,
        }

        mock_reward_config_params.update(reward_config_params)
        reward_config = RewardConfig(**mock_reward_config_params)
        db_session.add(reward_config)
        db_session.commit()

        return reward_config

    return _create_reward_config


@pytest.fixture(scope="function")
def reward(db_session: "Session", reward_config: RewardConfig) -> Reward:
    rc = Reward(
        code="TSTCD1234",
        retailer_id=reward_config.retailer_id,
        reward_config=reward_config,
    )
    db_session.add(rc)
    db_session.commit()
    return rc


@pytest.fixture(scope="function")
def create_mock_retailer(db_session: "Session", test_retailer: dict) -> Callable[..., Retailer]:
    def _create_mock_retailer(**retailer_params: Any) -> Retailer:  # noqa: ANN401
        """
        Create a retailer in the test DB
        :param retailer_params: override any values for the retailer, from what the mock_retailer fixture provides
        :return: Callable function
        """
        mock_retailer_params = deepcopy(test_retailer)

        mock_retailer_params.update(retailer_params)
        rtl = Retailer(**mock_retailer_params)
        db_session.add(rtl)
        db_session.commit()

        return rtl

    return _create_mock_retailer


@pytest.fixture(scope="function")
def campaign_balance(setup: SetupType, campaign: Campaign) -> CampaignBalance:
    db_session, _, account_holder = setup
    cmp_bal = CampaignBalance(
        account_holder_id=account_holder.id,
        campaign_id=campaign.id,
        balance=300,
    )
    db_session.add(cmp_bal)
    db_session.commit()
    return cmp_bal


@pytest.fixture(scope="function")
def create_balance(setup: SetupType, campaign: Campaign) -> Callable[..., CampaignBalance]:
    db_session, _, account_holder = setup
    data = {
        "account_holder_id": account_holder.id,
        "campaign_id": campaign.id,
        "balance": 300,
    }

    def _create_balance(**params: Any) -> CampaignBalance:  # noqa: ANN401
        data.update(params)
        cmp_bal = CampaignBalance(**data)
        db_session.add(cmp_bal)
        db_session.commit()
        return cmp_bal

    return _create_balance


@pytest.fixture(scope="function")
def pending_reward(setup: SetupType, campaign: Campaign) -> PendingReward:
    db_session, _, account_holder = setup
    pending_rwd = PendingReward(
        account_holder_id=account_holder.id,
        campaign_id=campaign.id,
        pending_reward_uuid=uuid4(),
        created_date=datetime(2022, 1, 1, 5, 0, tzinfo=timezone.utc),
        conversion_date=datetime.now(tz=timezone.utc) + timedelta(days=15),
        value=100,
        count=2,
        total_cost_to_user=300,
    )
    db_session.add(pending_rwd)
    db_session.commit()
    return pending_rwd


@pytest.fixture(scope="function")
def create_pending_reward(setup: SetupType, campaign: Campaign) -> Callable[..., PendingReward]:
    db_session, _, account_holder = setup
    data = {
        "account_holder_id": account_holder.id,
        "campaign_id": campaign.id,
        "pending_reward_uuid": uuid4(),
        "created_date": datetime(2022, 1, 1, 5, 0, tzinfo=timezone.utc),
        "conversion_date": datetime.now(tz=timezone.utc) + timedelta(days=15),
        "value": 100,
        "count": 2,
        "total_cost_to_user": 300,
    }

    def _create_pending_reward(**params: Any) -> PendingReward:  # noqa: ANN401
        data.update(params)
        pending_reward = PendingReward(**data)
        db_session.add(pending_reward)
        db_session.commit()

        return pending_reward

    return _create_pending_reward


@pytest.fixture(scope="function")
def create_mock_campaign(db_session: "Session", retailer: Retailer, mock_campaign: dict) -> Callable[..., Campaign]:
    def _create_mock_campaign(**campaign_params: dict) -> Campaign:
        """
        Create a campaign in the test DB
        :param campaign_params: override any values for the campaign, from what the mock_campaign fixture provides
        :return: Callable function
        """
        mock_campaign_params = deepcopy(mock_campaign)
        mock_campaign_params["retailer_id"] = retailer.id

        mock_campaign_params.update(campaign_params)
        cpn = Campaign(**mock_campaign_params)
        db_session.add(cpn)
        db_session.commit()

        return cpn

    return _create_mock_campaign


@pytest.fixture
def create_retailer_store(db_session: "Session") -> Callable:
    def _create_retailer_store(retailer_id: int, mid: str, store_name: str) -> RetailerStore:
        store = RetailerStore(retailer_id=retailer_id, mid=mid, store_name=store_name)
        db_session.add(store)
        db_session.commit()
        return store

    return _create_retailer_store


@pytest.fixture()
def create_transaction(db_session: "Session") -> Callable:
    def _create_transaction(account_holder: AccountHolder, **transaction_params: dict) -> Transaction:
        """
        :param transaction_params: transaction object values
        :return: Callable function
        """
        assert transaction_params["transaction_id"]
        transaction_data = {
            "account_holder_id": account_holder.id,
            "retailer_id": account_holder.retailer_id,
            "datetime": transaction_params.get("datetime", datetime(2022, 6, 1, 14, 30, 00, tzinfo=timezone.utc)),
            "transaction_id": transaction_params["transaction_id"],
            "amount": 1000,
            "processed": True,
        }
        transaction_data.update(transaction_params)
        transaction = Transaction(**transaction_data)
        db_session.add(transaction)
        db_session.commit()

        return transaction

    return _create_transaction


@pytest.fixture
def create_transaction_earn(db_session: "Session") -> Callable:
    def _create_transaction_earn(
        transaction: Transaction, earn_amount: str, loyalty_type: LoyaltyTypes, earn_rule: EarnRule
    ) -> TransactionEarn:
        te = TransactionEarn(
            transaction_id=transaction.id, earn_amount=earn_amount, loyalty_type=loyalty_type, earn_rule_id=earn_rule.id
        )
        db_session.add(te)
        db_session.commit()
        return te

    return _create_transaction_earn
