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
def test_jigsaw_agent_register_reversal_paths_no_previous_error_max_retries_exceeded(
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
    # deepcode ignore HardcodedNonCryptoSecret/test: this is a test value
    test_token = "test-token"
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(test_token.encode()), timedelta(days=1))
    mock_uuid4 = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    expected_last_val = uuid4()
    mock_uuid4.side_effect = (uuid4(), uuid4(), uuid4(), expected_last_val, uuid4())

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            self._update_calls_and_get_endpoint(uri)
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

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)

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

    assert answer_bot.calls["register"] == 4
    assert "reversal" not in answer_bot.calls
    assert exc_info.value.args[0] == (
        "Jigsaw: unknown error returned. status: 4000 Validation failed, endpoint: /order/V4/register, "
        f"message: 40028 order already exists, customer card ref: {expected_last_val}"
    )
    assert mock_uuid4.call_count == 4
    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    assert json.loads(task_params["agent_state_params_raw"])["customer_card_ref"] == str(expected_last_val)


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_max_retries_exceeded(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    set_reversal_true: None,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
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

                case "reversal":
                    return (
                        200,
                        response_headers,
                        json.dumps(
                            {
                                "status": 2000,
                                "status_description": "Success OK",
                                "messages": [],
                                "PartnerRef": "",
                                "data": None,
                            }
                        ),
                    )

                case _:
                    raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)

    with pytest.raises(AgentError):
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
    assert answer_bot.calls["reversal"] == 1

    assert mock_uuid.call_count == 4
    spy_redis_set.assert_not_called()

    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    agent_state_params = json.loads(task_params["agent_state_params_raw"])
    assert agent_state_params["customer_card_ref"] == str(card_ref)
    assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_need_new_token(
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
    card_ref = uuid4()
    success_card_ref = uuid4()
    card_num = "sample-reward-code"
    tx_value = jigsaw_reward_config.load_required_fields_values()["transaction_value"]
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_uuid.side_effect = [card_ref, success_card_ref]
    success_token = "test-token-success"
    now = datetime.now(tz=UTC)
    mock_datetime = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.datetime")
    mock_datetime.now.return_value = now
    mock_datetime.fromisoformat = datetime.fromisoformat

    class AnswerBot(AnswerBotBase):
        def response_generator(
            self, request: httpretty.core.HTTPrettyRequest, uri: str, response_headers: dict
        ) -> tuple[int, dict, str]:
            match self._update_calls_and_get_endpoint(uri):
                case "register":
                    if self.calls["reversal"] < 2:
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
                                    "customer_card_ref": json.loads(request.body)["customer_card_ref"],
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

                case "reversal":
                    if request.headers["token"] == success_token:
                        return (
                            200,
                            response_headers,
                            json.dumps(
                                {
                                    "status": 2000,
                                    "status_description": "Success OK",
                                    "messages": [],
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
                                    "Token": success_token,
                                    # jigsaw returns a naive datetime here
                                    "Expires": (now.replace(tzinfo=None) + timedelta(days=1)).isoformat(),
                                    "TestMode": True,
                                },
                            }
                        ),
                    )

                case _:
                    raise ValueError("should not have got here!")

    answer_bot = AnswerBot()
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/register", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/reversal", body=answer_bot.response_generator)
    httpretty.register_uri("POST", f"{agent_config['base_url']}/order/V4/getToken", body=answer_bot.response_generator)

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

    assert answer_bot.calls["register"] == 2
    assert answer_bot.calls["reversal"] == 2
    assert answer_bot.calls["getToken"] == 1

    assert mock_uuid.call_count == 2
    spy_redis_set.assert_called_once()

    assert success
    reward: Reward = db_session.execute(select(Reward).where(Reward.reward_uuid == success_card_ref)).scalar_one()

    assert reward.code == card_num
    assert reward.issued_date == now.replace(tzinfo=None)
    assert reward.expiry_date == (now + timedelta(days=1)).replace(tzinfo=None)
    assert reward.account_holder_id == account_holder.id

    db_session.refresh(jigsaw_reward_issuance_task)
    task_params = jigsaw_reward_issuance_task.get_params()
    agent_state_params = json.loads(task_params["agent_state_params_raw"])

    assert agent_state_params["customer_card_ref"] == str(success_card_ref)
    assert agent_state_params["reversal_customer_card_ref"] == str(card_ref)
    assert agent_state_params["might_need_reversal"] is False
    mock_issued_reward_activity.assert_called()


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_retry_paths(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    set_reversal_true: None,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    card_ref = uuid4()
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
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
        status=200,
    )
    for expected_call_count, (jigsaw_status, description, expected_status) in enumerate(
        (
            (5000, "Internal Server Error", status.HTTP_500_INTERNAL_SERVER_ERROR),
            (5003, "Service Unavailable", status.HTTP_503_SERVICE_UNAVAILABLE),
        ),
        start=2,
    ):
        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/reversal",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
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

        assert exc_info.value.response.status_code == expected_status  # type: ignore [union-attr]

        assert mock_uuid.call_count == expected_call_count
        spy_redis_set.assert_not_called()

        db_session.refresh(jigsaw_reward_issuance_task)
        task_params = jigsaw_reward_issuance_task.get_params()
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert agent_state_params["customer_card_ref"] == str(card_ref)
        assert agent_state_params["might_need_reversal"] is True


@httpretty.activate
def test_jigsaw_agent_register_reversal_paths_previous_error_failure_paths(
    fernet: "Fernet",
    mocker: "MockerFixture",
    db_session: "Session",
    jigsaw_reward_config: "RewardConfig",
    jigsaw_retailer_fetch_type: "RetailerFetchType",
    jigsaw_reward_issuance_task: "RetryTask",
    account_holder: "AccountHolder",
    jigsaw_campaign: "Campaign",
    set_reversal_true: None,
) -> None:
    agent_config = jigsaw_retailer_fetch_type.load_agent_config()
    card_ref = uuid4()
    redis_raw.set(Jigsaw.REDIS_TOKEN_KEY, fernet.encrypt(b"test-token"), timedelta(days=1))
    spy_redis_set = mocker.spy(redis_raw, "set")
    card_ref = uuid4()
    mock_uuid = mocker.patch("cosmos.rewards.fetch_reward.jigsaw.uuid4")
    mock_uuid.return_value = card_ref

    httpretty.register_uri(
        "POST",
        f"{agent_config['base_url']}/order/V4/register",
        body=json.dumps(
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
        status=200,
    )

    for expected_call_count, (jigsaw_status, description, expected_status) in enumerate(
        (
            (4003, "Forbidden", status.HTTP_403_FORBIDDEN),
            (4001, "Unauthorised", status.HTTP_401_UNAUTHORIZED),
        ),
        start=2,
    ):
        httpretty.register_uri(
            "POST",
            f"{agent_config['base_url']}/order/V4/reversal",
            body=json.dumps(
                {
                    "status": jigsaw_status,
                    "status_description": description,
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

        assert exc_info.value.response.status_code == expected_status  # type: ignore [union-attr]

        assert mock_uuid.call_count == expected_call_count
        spy_redis_set.assert_not_called()

        db_session.refresh(jigsaw_reward_issuance_task)
        task_params = jigsaw_reward_issuance_task.get_params()
        agent_state_params = json.loads(task_params["agent_state_params_raw"])
        assert agent_state_params["customer_card_ref"] == str(card_ref)
        assert agent_state_params["might_need_reversal"] is True
