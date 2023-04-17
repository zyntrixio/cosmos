import json

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast
from unittest import mock
from uuid import uuid4

import httpretty

from retry_tasks_lib.db.models import TaskTypeKey, TaskTypeKeyValue
from sqlalchemy.future import select

from cosmos.core.config import redis_raw
from cosmos.db.models import Reward
from cosmos.rewards.fetch_reward.jigsaw import Jigsaw
from cosmos.rewards.schemas import IssuanceTaskParams

from . import AnswerBotBase

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    from cryptography.fernet import Fernet
    from pytest_mock import MockerFixture
    from retry_tasks_lib.db.models import RetryTask
    from sqlalchemy.orm import Session

    from cosmos.db.models import AccountHolder, Campaign, RetailerFetchType, RewardConfig


@httpretty.activate
def test_jigsaw_agent_ok(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    mock_issued_reward_activity: "MagicMock",
) -> None:
    sample_url = "http://sample.url"
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=UTC)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/getToken",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response.getToken:#Jigsaw.API.Service",
                    "Token": test_token,
                    # jigsaw returns a naive datetime here
                    "Expires": (now.replace(tzinfo=None) + timedelta(days=1)).isoformat(),
                    "TestMode": True,
                },
            }
        ),
        status=200,
    )
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
                    "reference": "339069",
                    "number": card_num,
                    "pin": "",
                    "transaction_value": tx_value,
                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                    "balance": tx_value,
                    "voucher_url": sample_url,
                    "card_status": 1,
                },
            }
        ),
        status=200,
    )
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with Jigsaw(
        db_session,
        campaign=jigsaw_campaign,
        reward_config=jigsaw_reward_config,
        account_holder=account_holder,
        config=agent_config,
        retry_task=jigsaw_reward_issuance_task,
        task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
    ) as agent:
        assert agent.issue_reward() == 1

    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id
    assert reward.associated_url == sample_url

    mock_uuid.assert_called_once()
    spy_redis_set.assert_called_once_with(Jigsaw.REDIS_TOKEN_KEY, mock.ANY, timedelta(days=1))
    assert fernet.decrypt(cast(bytes, redis_raw.get(Jigsaw.REDIS_TOKEN_KEY))).decode() == test_token

    mock_issued_reward_activity.assert_called()


@httpretty.activate
def test_jigsaw_agent_ok_token_already_set(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    mock_issued_reward_activity: "MagicMock",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    sample_url = "http://sample.url"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=UTC)
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
                    "reference": "339069",
                    "number": card_num,
                    "pin": "",
                    "transaction_value": tx_value,
                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                    "balance": tx_value,
                    "voucher_url": sample_url,
                    "card_status": 1,
                },
            }
        ),
        status=200,
    )

    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")

    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    with Jigsaw(
        db_session,
        campaign=jigsaw_campaign,
        reward_config=jigsaw_reward_config,
        account_holder=account_holder,
        config=agent_config,
        retry_task=jigsaw_reward_issuance_task,
        task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
    ) as agent:
        assert agent.issue_reward() == 1

    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id
    assert reward.associated_url == sample_url

    mock_uuid.assert_called_once()
    spy_redis_set.assert_not_called()

    mock_issued_reward_activity.assert_called()


@httpretty.activate
def test_jigsaw_agent_ok_card_ref_in_task_params(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    mock_issued_reward_activity: "MagicMock",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=UTC)
    sample_url = "http://sample.url"
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 2000,
                "status_description": "OK",
                "messages": [],
                "PartnerRef": "",
                "data": {
                    "__type": "Response_Data.cardData:#Order_V4",
                    "customer_card_ref": str(card_ref),
                    "reference": "339069",
                    "number": card_num,
                    "pin": "",
                    "transaction_value": tx_value,
                    "expiry_date": (now + timedelta(days=1)).isoformat(),
                    "balance": tx_value,
                    "voucher_url": sample_url,
                    "card_status": 1,
                },
            }
        ),
        status=200,
    )
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    db_session.execute(
        TaskTypeKeyValue.__table__.insert().values(
            value=json.dumps({"customer_card_ref": str(card_ref)}),
            retry_task_id=jigsaw_reward_issuance_task.retry_task_id,
            task_type_key_id=(
                select(TaskTypeKey.task_type_key_id)
                .where(
                    TaskTypeKey.task_type_id == jigsaw_reward_issuance_task.task_type_id,
                    TaskTypeKey.name == "agent_state_params_raw",
                )
                .scalar_subquery()
            ),
        ),
    )
    db_session.commit()

    with Jigsaw(
        db_session,
        campaign=jigsaw_campaign,
        reward_config=jigsaw_reward_config,
        account_holder=account_holder,
        config=agent_config,
        retry_task=jigsaw_reward_issuance_task,
        task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
    ) as agent:
        assert agent.issue_reward() == 1

    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id

    mock_uuid.assert_not_called()
    spy_redis_set.assert_not_called()

    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    assert "might_need_reversal" not in task_params.get("agent_state_params_raw", {})

    mock_issued_reward_activity.assert_called()


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_no_previous_error_ok(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    mock_issued_reward_activity: "MagicMock",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    now = datetime.now(tz=UTC)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    sample_url = "https://sample.url"
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    successful_card_ref = uuid4()
    mock_uuid.side_effect = [card_ref, successful_card_ref]
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:

            self._update_calls_and_get_endpoint(uri)
            requests_card_ref = json.loads(request.body)["customer_card_ref"]

            if requests_card_ref == str(card_ref):
                return (
                    200,
                    response_headers,
                    json.dumps(
                        {
                            "status": 4000,
                            "status_description": "Validation failed",
                            "messages": [
                                {
                                    "isError": True,
                                    "id": "40028",
                                    "Info": "order already exists",
                                }
                            ],
                            "PartnerRef": "",
                            "data": None,
                        }
                    ),
                )

            return (
                200,
                response_headers,
                json.dumps(
                    {
                        "status": 2000,
                        "status_description": "OK",
                        "messages": [],
                        "PartnerRef": "",
                        "data": {
                            "__type": "Response_Data.cardData:#Order_V4",
                            "customer_card_ref": requests_card_ref,
                            "reference": "339069",
                            "number": card_num,
                            "pin": "",
                            "transaction_value": tx_value,
                            "expiry_date": (now + timedelta(days=1)).isoformat(),
                            "balance": tx_value,
                            "voucher_url": sample_url,
                            "card_status": 1,
                        },
                    }
                ),
            )

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)

    with Jigsaw(
        db_session,
        campaign=jigsaw_campaign,
        reward_config=jigsaw_reward_config,
        account_holder=account_holder,
        config=agent_config,
        retry_task=jigsaw_reward_issuance_task,
        task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
    ) as agent:
        assert agent.issue_reward() == 1

    assert answer_bot.calls["register"] == 2
    assert "reversal" not in answer_bot.calls

    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == successful_card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id

    assert mock_uuid.call_count == 2
    spy_redis_set.assert_not_called()

    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(successful_card_ref)


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_ok(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    mock_issued_reward_activity: "MagicMock",
    set_reversal_true: None,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    card_ref = uuid4()
    card_num = "NEW-REWARD-CODE"
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    sample_url = "https://sample.url"
    now = datetime.now(tz=UTC)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")

    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    successful_card_ref = uuid4()
    mock_uuid.side_effect = [card_ref, successful_card_ref]
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:

            endpoint = self._update_calls_and_get_endpoint(uri)

            if endpoint == "register":
                requests_card_ref = json.loads(request.body)["customer_card_ref"]

                if requests_card_ref == str(card_ref):
                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 4000,
                                "status_description": "Validation failed",
                                "messages": [
                                    {
                                        "isError": True,
                                        "id": "40028",
                                        "Info": "order already exists",
                                    }
                                ],
                                "PartnerRef": "",
                                "data": None,
                            }
                        ),
                    )

                return (
                    200,
                    response_headers,
                    json.dumps(
                        {
                            "status": 2000,
                            "status_description": "OK",
                            "messages": [],
                            "PartnerRef": "",
                            "data": {
                                "__type": "Response_Data.cardData:#Order_V4",
                                "customer_card_ref": requests_card_ref,
                                "reference": "339069",
                                "number": card_num,
                                "pin": "",
                                "transaction_value": tx_value,
                                "expiry_date": (now + timedelta(days=1)).isoformat(),
                                "balance": tx_value,
                                "voucher_url": sample_url,
                                "card_status": 1,
                            },
                        }
                    ),
                )

            if endpoint == "reversal":
                return (
                    200,
                    response_headers,
                    json.dumps(
                        {
                            "status": 2000,
                            "status_description": "OK",
                            "messages": [],
                            "PartnerRef": "",
                            "data": None,
                        }
                    ),
                )

            raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)

    with Jigsaw(
        db_session,
        campaign=jigsaw_campaign,
        reward_config=jigsaw_reward_config,
        account_holder=account_holder,
        config=agent_config,
        retry_task=jigsaw_reward_issuance_task,
        task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
    ) as agent:
        assert agent.issue_reward() == 1

    assert answer_bot.calls["register"] == 2
    assert answer_bot.calls["reversal"] == 1

    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == successful_card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id

    assert mock_uuid.call_count == 2
    spy_redis_set.assert_not_called()

    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert agent_state_params.get("customer_card_ref") == str(successful_card_ref)
    assert agent_state_params.get("reversal_customer_card_ref") == str(card_ref)
    assert agent_state_params["might_need_reversal"] is False

    mock_issued_reward_activity.assert_called()
