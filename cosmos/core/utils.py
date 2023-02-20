# import logging
import random

from babel.numbers import format_currency

from cosmos.campaigns.enums import LoyaltyTypes

# from uuid import uuid4

# from retry_tasks_lib.db.models import RetryTask
# from retry_tasks_lib.utils.synchronous import enqueue_many_retry_tasks, sync_create_many_tasks


# from cosmos.core.config import redis_raw, settings
# from cosmos.db.base_class import sync_run_query
# from cosmos.db.models import PendingReward


MINIMUM_ACCOUNT_NUMBER_LENGTH = 10


def generate_account_number(prefix: str, number_length: int = MINIMUM_ACCOUNT_NUMBER_LENGTH) -> str:
    prefix = prefix.strip().upper()
    if not prefix.isalnum():
        raise ValueError("prefix is not alpha-numeric")
    if number_length < MINIMUM_ACCOUNT_NUMBER_LENGTH:
        raise ValueError(f"minimum card number length is {MINIMUM_ACCOUNT_NUMBER_LENGTH}")
    start, end = 1, (10**number_length) - 1
    return f"{prefix}{str(random.randint(start, end)).zfill(number_length)}"


def pence_integer_to_currency_string(value: int, currency: str, currency_sign: bool = True) -> str:
    extras = {} if currency_sign else {"format": "#,##0.##"}
    return format_currency(value / 100, currency, locale="en_GB", **extras)


def build_tx_history_reasons(tx_amount: int, adjustments: dict, is_refund: bool, currency: str) -> list[str]:
    reasons = []
    for v in adjustments.values():

        amount = pence_integer_to_currency_string(abs(tx_amount), currency)
        threshold = pence_integer_to_currency_string(v["threshold"], currency)

        if v["accepted"]:
            if is_refund:
                reasons.append(f"refund of {amount} accepted")
            else:
                reasons.append(f"transaction amount {amount} meets the required threshold {threshold}")
        elif is_refund:
            reasons.append(f"refund of {amount} not accepted")
        else:
            reasons.append(f"transaction amount {amount} does no meet the required threshold {threshold}")

    return reasons


def humanize_earn_amount(amount: int, loyalty_type: LoyaltyTypes, currency: str, currency_sign: bool = True) -> str:
    if loyalty_type == LoyaltyTypes.ACCUMULATOR:
        val = pence_integer_to_currency_string(amount, currency, currency_sign=currency_sign)
    elif loyalty_type == LoyaltyTypes.STAMPS:
        val = str(amount // 100)
    return val


def build_tx_history_earns(adjustments: dict, currency: str) -> list[dict[str, str]]:
    earns = []
    for v in adjustments.values():
        amount = humanize_earn_amount(v["amount"], v["type"], currency, currency_sign=True)
        earns.append({"value": amount, "type": v["type"]})
    return earns


# def set_param_value(
#     db_session: "Session", retry_task: RetryTask, param_name: str, param_value: Any, commit: bool = True
# ) -> str:
#     def _query() -> str:
#         key_ids_by_name = retry_task.task_type.get_key_ids_by_name()
#         task_type_key_val = retry_task.get_task_type_key_values([(key_ids_by_name[param_name], param_value)])[0]
#         db_session.add(task_type_key_val)
#         if commit:
#             db_session.commit()

#         return task_type_key_val.value

#     return sync_run_query(_query, db_session)
