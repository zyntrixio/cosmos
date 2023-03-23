import random

from babel.numbers import format_currency

from cosmos.campaigns.enums import LoyaltyTypes

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
    return format_currency(value / 100, currency, locale="en_GB", format=None if currency_sign else "#,##0.##")


def raw_stamp_value_to_string(value: int, stamp_suffix: bool = True) -> str:
    stamp_val = value // 100
    suffix = f" stamp{'s' if abs(stamp_val) != 1 else ''}"
    return f"{stamp_val}{suffix if stamp_suffix else ''}"


def get_formatted_balance_by_loyalty_type(value: int, loyalty_type: LoyaltyTypes, sign: bool = True) -> str:
    match loyalty_type:
        case LoyaltyTypes.STAMPS:
            return raw_stamp_value_to_string(value, stamp_suffix=sign)
        case LoyaltyTypes.ACCUMULATOR:
            return pence_integer_to_currency_string(value, "GBP", currency_sign=sign)

    raise ValueError(f"Unexpected value {loyalty_type} for loyalty_type.")
