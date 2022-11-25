from babel.numbers import format_currency


def pence_integer_to_currency_string(value: int, currency: str, currency_sign: bool = True) -> str:
    extras: dict = {}
    if not currency_sign:
        extras = {"format": "#,##0.##"}

    return format_currency(value / 100, currency, locale="en_GB", **extras)
