import json

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import choice, randint
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from faker import Faker

from .enums import AccountHolderRewardStatuses
from .utils import generate_account_number, generate_email

if TYPE_CHECKING:
    from cosmos.db.models import AccountHolder, Retailer

    from .enums import AccountHolderTypes


fake = Faker(["en-GB"])

ACCOUNT_HOLDER_REWARD_SWITCHER: dict[int, list] = {
    0: [],
    1: [(1, AccountHolderRewardStatuses.ISSUED)],
    2: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.EXPIRED),
    ],
    3: [
        (3, AccountHolderRewardStatuses.ISSUED),
        (3, AccountHolderRewardStatuses.REDEEMED),
    ],
    4: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.CANCELLED),
    ],
    5: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.PENDING),
    ],
    6: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.EXPIRED),
        (1, AccountHolderRewardStatuses.REDEEMED),
        (1, AccountHolderRewardStatuses.CANCELLED),
        (1, AccountHolderRewardStatuses.PENDING),
    ],
    7: [
        (2, AccountHolderRewardStatuses.ISSUED),
    ],
    8: [
        (3, AccountHolderRewardStatuses.ISSUED),
        (1, AccountHolderRewardStatuses.EXPIRED),
        (3, AccountHolderRewardStatuses.REDEEMED),
        (2, AccountHolderRewardStatuses.CANCELLED),
        (3, AccountHolderRewardStatuses.PENDING),
    ],
    9: [
        (1, AccountHolderRewardStatuses.ISSUED),
        (3, AccountHolderRewardStatuses.EXPIRED),
        (2, AccountHolderRewardStatuses.REDEEMED),
        (1, AccountHolderRewardStatuses.CANCELLED),
        (4, AccountHolderRewardStatuses.PENDING),
    ],
    10: [
        (2, AccountHolderRewardStatuses.ISSUED),
        (4, AccountHolderRewardStatuses.EXPIRED),
        (3, AccountHolderRewardStatuses.REDEEMED),
        (1, AccountHolderRewardStatuses.CANCELLED),
        (5, AccountHolderRewardStatuses.PENDING),
    ],
    11: [
        (3, AccountHolderRewardStatuses.PENDING),
    ],
}


@dataclass
class TxHistoryRowsData:
    tx_amount: int
    mid: str


def generate_tx_rows(reward_goal: int, retailer: "Retailer") -> list[TxHistoryRowsData]:
    mids = [store.mid for store in retailer.stores]
    tx_history_list = [
        TxHistoryRowsData(int((reward_goal // 4)), choice(mids)),
        TxHistoryRowsData(int(-(reward_goal // 2)), choice(mids)),
        TxHistoryRowsData(int(reward_goal // 2), choice(mids)),
        TxHistoryRowsData(int(reward_goal), choice(mids)),
        TxHistoryRowsData(int(-reward_goal), choice(mids)),
        TxHistoryRowsData(int(reward_goal * 1.5), choice(mids)),
        TxHistoryRowsData(int(-(reward_goal * 1.5)), choice(mids)),
        TxHistoryRowsData(int(-(reward_goal * 2)), choice(mids)),
        TxHistoryRowsData(int(reward_goal * 2), choice(mids)),
        TxHistoryRowsData(int(-(reward_goal // 4)), choice(mids)),
    ]
    return tx_history_list


def account_holder_payload(
    account_holder_n: int, account_holder_type: "AccountHolderTypes", retailer: "Retailer"
) -> dict:
    return {
        "email": generate_email(account_holder_type, account_holder_n),
        "retailer_id": retailer.id,
        "status": "ACTIVE",
        "account_number": generate_account_number(
            retailer.account_number_prefix, account_holder_type, account_holder_n
        ),
    }


def account_holder_profile_payload(account_holder: "AccountHolder") -> dict:
    phone_prefix = "0" if randint(0, 1) else "+44"
    address = fake.street_address().split("\n")
    address_1 = address[0]
    if len(address) > 1:
        address_2 = address[1]
    else:
        address_2 = ""

    return {
        "account_holder_id": account_holder.id,
        "date_of_birth": fake.date(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "phone": phone_prefix + fake.msisdn(),
        "address_line1": address_1,
        "address_line2": address_2,
        "postcode": fake.postcode(),
        "city": fake.city(),
    }


def marketing_preference_payload(account_holder: "AccountHolder") -> dict:
    return {
        "account_holder_id": account_holder.id,
        "key_name": "marketing_pref",
        "value": "False",
        "value_type": "BOOLEAN",
    }


def account_holder_reward_payload(
    account_holder_id: int,
    retailer_id: int,
    campaign_id: int,
    reward_uuid: UUID,
    reward_code: str,
    reward_config_id: int,
    reward_status: AccountHolderRewardStatuses,
    issue_date: datetime,
) -> dict:
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)

    return {
        "account_holder_id": account_holder_id,
        "retailer_id": retailer_id,
        "campaign_id": campaign_id,
        "reward_uuid": reward_uuid,
        "code": reward_code,
        "reward_config_id": reward_config_id,
        # "status": AccountHolderRewardStatuses.ISSUED.value
        # if reward_status == AccountHolderRewardStatuses.EXPIRED
        # else reward_status.value,
        "issued_date": issue_date,
        "expiry_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.EXPIRED
        else datetime(2030, 1, 1),
        "redeemed_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.REDEEMED
        else None,
        "cancelled_date": now - timedelta(days=randint(2, 10))
        if reward_status == AccountHolderRewardStatuses.CANCELLED
        else None,
    }


def account_holder_pending_reward_payload(
    account_holder_id: int,
    campaign_id: str,
    refund_window: int,
) -> dict:
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)
    pending_reward_value = 200
    count = 1

    return {
        "created_date": now,
        "conversion_date": now + timedelta(days=refund_window),
        "value": 200,
        "account_holder_id": account_holder_id,
        "campaign_id": campaign_id,
        "count": count,
        "total_cost_to_user": pending_reward_value * count,
        "pending_reward_uuid": str(uuid4()),
    }


def account_holder_transaction_payload(
    retailer_id: int,
    account_holder_id: int,
    tx_amount: int,
    mid: str,
) -> dict:
    now = datetime.now(tz=timezone.utc).replace(microsecond=0)

    return {
        "transaction_id": f"{account_holder_id}{randint(1, 1000000)}",
        "datetime": now,
        "amount": tx_amount,
        "mid": mid,
        "retailer_id": retailer_id,
        "account_holder_id": account_holder_id,
        "processed": True,
    }


def reward_payload(reward_code: str, reward_config_id: int, retailer_id: int) -> dict:
    return {
        "code": reward_code,
        "reward_config_id": reward_config_id,
        "retailer_id": retailer_id,
        "issued_date": datetime.now(tz=timezone.utc),
        "deleted": False,
    }


def retailer_data(retailer_slug: str) -> dict:
    retailer_name = retailer_slug.replace("-", " ").title()
    return {
        "name": retailer_name,
        "slug": retailer_slug,
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
        "loyalty_name": retailer_name,
        "status": "TEST",
    }


def campaign_payload(retailer_id: int, campaign_slug: str, loyalty_type: str, reward_config_id: int) -> dict:
    return {
        "retailer_id": retailer_id,
        "status": "ACTIVE",
        "name": campaign_slug.replace("-", " ").title(),
        "slug": campaign_slug,
        "start_date": datetime.now(tz=timezone.utc) - timedelta(minutes=5),
        "loyalty_type": loyalty_type,
        "reward_config_id": reward_config_id,
    }


def reward_rule_payload(campaign_id: int, reward_config_id: int, refund_window: int | None) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "reward_config_id": reward_config_id,
        "reward_goal": 1000,
    }
    if refund_window is not None:
        payload.update({"allocation_window": refund_window})

    return payload


def earn_rule_payload(campaign_id: int, loyalty_type: str) -> dict:
    return {
        "campaign_id": campaign_id,
        "threshold": 500,
        "increment": 300 if loyalty_type == "STAMPS" else None,
        "increment_multiplier": 1.25,
    }


def reward_config_payload(retailer_id: int, fetch_type_id: int) -> dict:
    payload = {
        "retailer_id": retailer_id,
        "status": "ACTIVE",
        "fetch_type_id": fetch_type_id,
        "required_fields_values": json.dumps({"validity_days": 30}),
    }
    return payload


def retailer_fetch_type_payload(retailer_id: int, fetch_type_id: int, agent_config: str | None = None) -> dict:
    payload: dict[str, Any] = {"retailer_id": retailer_id, "fetch_type_id": fetch_type_id}
    if agent_config:
        payload["agent_config"] = agent_config
    return payload
