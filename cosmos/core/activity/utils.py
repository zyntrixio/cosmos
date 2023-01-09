from typing import TYPE_CHECKING, Callable, Iterable, TypeVar

from babel.numbers import format_currency

from cosmos.core.activity.tasks import async_send_activity

from . import logger

if TYPE_CHECKING:
    from enum import Enum

    from fastapi import BackgroundTasks

    ActivityEnumType = TypeVar("ActivityEnumType", bound="Enum")


def pence_integer_to_currency_string(value: int, currency: str, currency_sign: bool = True) -> str:
    extras = {} if currency_sign else {"format": "#,##0.##"}
    return format_currency(value / 100, currency, locale="en_GB", **extras)


async def format_and_send_activity_in_background(
    background_tasks: "BackgroundTasks",
    *,
    activity_type: "ActivityEnumType",
    payload_formatter_fn: Callable[..., dict],
    formatter_kwargs: list[dict] | dict,
) -> None:
    async def _background_task(
        activity_type: "ActivityEnumType",
        payload_formatter_fn: Callable[..., dict],
        formatter_kwargs: list[dict] | dict,
    ) -> None:

        try:
            payload: Iterable[dict] | dict
            if isinstance(formatter_kwargs, dict):  # noqa SIM108
                payload = payload_formatter_fn(**formatter_kwargs)
            else:
                payload = (payload_formatter_fn(**instance_kwargs) for instance_kwargs in formatter_kwargs)

            await async_send_activity(payload, routing_key=activity_type.value)

        except Exception:  # noqa BLE001
            logger.exception(
                "Failed to send %s activities for with provided kwargs:\n%s", activity_type.name, formatter_kwargs
            )

    background_tasks.add_task(_background_task, activity_type, payload_formatter_fn, formatter_kwargs)
