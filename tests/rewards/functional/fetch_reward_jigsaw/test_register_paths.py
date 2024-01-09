import json

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

import httpretty
import pytest
import requests

from fastapi import status
from sqlalchemy.future import select

from cosmos.core.config import redis_raw
from cosmos.db.models import Reward
from cosmos.rewards.fetch_reward.base import AgentError
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
def test_jigsaw_agent_register_retry_paths(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"

    now = datetime.now(tz=UTC)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    for jigsaw_status, description, expected_status in (
        (5000, "Internal Server Error", status.HTTP_500_INTERNAL_SERVER_ERROR),
        (5003, "Service Unavailable", status.HTTP_503_SERVICE_UNAVAILABLE),
    ):
        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/register",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
                    "messages": [
                        {
                            "isError": True,
                            "id": "5",
                            "Info": "RetryableError",
                        }
                    ],
                }
            ),
            status=200,
        )

        with pytest.raises(requests.RequestException) as exc_info:
            with Jigsaw(
                db_session,
                campaign=jigsaw_campaign,
                reward_config=jigsaw_reward_config,
                account_holder=account_holder,
                config=agent_config,
                retry_task=jigsaw_reward_issuance_task,
                task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
            ) as agent:
                agent.issue_reward()

        assert exc_info.value.response.status_code == expected_status  # type: ignore [union-attr]
        mock_uuid.assert_called()
        spy_redis_set.assert_not_called()
        db_session.refresh(jigsaw_reward_issuance_task)
        task_params = jigsaw_reward_issuance_task.get_params()

        assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(card_ref)

        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert "customer_card_ref" in agent_state_params
        assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_failure_paths(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"

    now = datetime.now(tz=UTC)
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
            {
                "status": 4001,
                "status_description": "Unauthorised",
                "messages": [
                    {
                        "isError": True,
                        "id": "30001",
                        "Info": "Access denied",
                    }
                ],
            }
        ),
        status=200,
    )

    with pytest.raises(requests.RequestException) as exc_info:
        with Jigsaw(
            db_session,
            campaign=jigsaw_campaign,
            reward_config=jigsaw_reward_config,
            account_holder=account_holder,
            config=agent_config,
            retry_task=jigsaw_reward_issuance_task,
            task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
        ) as agent:
            agent.issue_reward()

    assert exc_info.value.response.status_code == status.HTTP_401_UNAUTHORIZED  # type: ignore [union-attr]
    mock_uuid.assert_called()
    spy_redis_set.assert_not_called()
    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()

    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert agent_state_params["customer_card_ref"] == str(card_ref)
    assert "might_need_reversal" not in agent_state_params


@httpretty.activate
def test_jigsaw_agent_register_unexpected_error_response(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        status=200,
        body=json.dumps(
            {
                "status": 9000,
                "status_description": "OMG",
                "messages": [
                    {
                        "isError": True,
                        "id": "9000",
                        "Info": "AHHHHHHHHHHHH!!!!",
                    }
                ],
            }
        ),
    )

    spy_redis_set = mocker.spy(redis_raw, "set")
    spy_logger = mocker.spy(Jigsaw, "logger")

    with pytest.raises(AgentError) as exc_info:
        with Jigsaw(
            db_session,
            campaign=jigsaw_campaign,
            reward_config=jigsaw_reward_config,
            account_holder=account_holder,
            config=agent_config,
            retry_task=jigsaw_reward_issuance_task,
            task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
        ) as agent:
            agent.issue_reward()

    spy_logger.exception.assert_called_with(
        "Exception occurred while fetching a new Jigsaw reward or cleaning up an existing task, "
        "exiting agent gracefully.",
        exc_info=exc_info.value,
    )
    assert exc_info.value.args[0] == (
        "Jigsaw: unknown error returned. status: 9000 OMG, endpoint: /order/V4/register, "
        f"message: 9000 AHHHHHHHHHHHH!!!!, customer card ref: {card_ref}"
    )
    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()

    spy_redis_set.assert_not_called()

    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" in agent_state_params
    assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_timeout_response(
    fernet: "Fernet",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))

    def timeout_response(request: requests.Request, uri: str, response_headers: dict) -> None:
        raise requests.Timeout("too bad")

    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=timeout_response)

    with pytest.raises(requests.RequestException):
        with Jigsaw(
            db_session,
            campaign=jigsaw_campaign,
            reward_config=jigsaw_reward_config,
            account_holder=account_holder,
            config=agent_config,
            retry_task=jigsaw_reward_issuance_task,
            task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
        ) as agent:
            agent.issue_reward()

    assert db_session.scalar(select(Reward).where(Reward.reward_config_id == jigsaw_reward_config.id)) is None
    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()

    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" in agent_state_params
    assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_retry_get_token_success(
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
    retry_error_ids = ["10003", "10006", "10007"]
    tx_value = 15
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    card_num = "NEW-REWARD-CODE"
    now = datetime.now(tz=UTC)
    mock_redis = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.redis_raw")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4", return_value=card_ref)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat
    get_token_url = f"{agent_config['base_url']}/order/V4/getToken"
    register_url = f"{agent_config['base_url']}/order/V4/register"

    httpretty.register_uri(
        "POST",
        get_token_url,
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

    def register_response_generator(
        request: requests.Request, uri: str, response_headers: dict
    ) -> tuple[int, dict, str]:
        for msg_id in retry_error_ids:
            if request.headers.get("Token") == f"invalid-token-{msg_id}":
                return (
                    200,
                    response_headers,
                    json.dumps(
                        {
                            "status": 4001,
                            "status_description": "Unauthorised",
                            "messages": [
                                {
                                    "isError": True,
                                    "id": msg_id,
                                    "Info": "Token invalid",
                                }
                            ],
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
                        "customer_card_ref": str(card_ref),
                        "reference": "339069",
                        "number": card_num,
                        "pin": "",
                        "transaction_value": tx_value,
                        "expiry_date": (now + timedelta(days=1)).isoformat(),
                        "balance": tx_value,
                        "voucher_url": "https://sample.url",
                        "card_status": 1,
                    },
                }
            ),
        )

    httpretty.register_uri("POST", register_url, body=register_response_generator)

    def encrypted(token: str) -> bytes:
        return fernet.encrypt(token.encode())

    mock_redis.get.side_effect = [
        encrypted("invalid-token-10003"),
        None,
        encrypted("invalid-token-10006"),
        None,
        encrypted("invalid-token-10007"),
        None,
    ]
    for _ in retry_error_ids:
        with Jigsaw(
            db_session,
            campaign=jigsaw_campaign,
            reward_config=jigsaw_reward_config,
            account_holder=account_holder,
            config=agent_config,
            retry_task=jigsaw_reward_issuance_task,
            task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
        ) as agent:
            success = agent.issue_reward()

        mock_uuid.assert_called()
        mock_redis.set.assert_called()
        mock_redis.get.assert_called()
        mock_redis.delete.assert_called()

        db_session.refresh(jigsaw_reward_issuance_task)
        audit = jigsaw_reward_issuance_task.audit_data
        assert audit[0]["request"]["url"] == register_url
        assert audit[0]["response"]["jigsaw_status"] == "4001 Unauthorised"
        assert audit[1]["request"]["url"] == get_token_url
        assert audit[1]["response"]["jigsaw_status"] == "2000 OK"
        assert audit[2]["request"]["url"] == register_url
        assert audit[2]["response"]["jigsaw_status"] == "2000 OK"

        assert success

        reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == card_ref)).scalar_one()

        assert reward.code == card_num
        assert reward.issued_date == now.replace(tzinfo=None)
        assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
        assert reward.account_holder_id == account_holder.id
        mock_issued_reward_activity.assert_called()

        mock_issued_reward_activity.reset_mock()
        jigsaw_reward_issuance_task.audit_data = []
        db_session.delete(reward)
        db_session.commit()


@httpretty.activate
def test_jigsaw_agent_register_retry_get_token_max_retries_exceeded(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    # mock_issued_reward_activity: "MagicMock",
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            match self._update_calls_and_get_endpoint(uri):
                case "register":
                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 4001,
                                "status_description": "Unauthorised",
                                "messages": [
                                    {
                                        "isError": True,
                                        "id": "10003",
                                        "Info": "Token invalid",
                                    }
                                ],
                            }
                        ),
                    )
                case "getToken":
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
                                    "__type": "Response.getToken:#Jigsaw.API.Service",
                                    "Token": "test-token",
                                    # jigsaw returns a naive datetime here
                                    "Expires": (datetime.now(tz=UTC) + timedelta(days=1)).isoformat(),
                                    "TestMode": True,
                                },
                            }
                        ),
                    )

                case _:
                    raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/getToken", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)

    with pytest.raises(requests.RequestException) as exc_info:
        with Jigsaw(
            db_session,
            campaign=jigsaw_campaign,
            reward_config=jigsaw_reward_config,
            account_holder=account_holder,
            config=agent_config,
            retry_task=jigsaw_reward_issuance_task,
            task_params=IssuanceTaskParams(**jigsaw_reward_issuance_task.get_params()),
        ) as agent:
            agent.issue_reward()

    assert answer_bot.calls["register"] == 4
    assert answer_bot.calls["getToken"] == 4

    assert exc_info.value.args[0] == (
        "Received a 4001 Unauthorised response. endpoint: /order/V4/register, message: 10003 Token invalid, "
        f"customer card ref: {card_ref}"
    )
    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert "customer_card_ref" in agent_state_params
    assert "might_need_reversal" not in agent_state_params
