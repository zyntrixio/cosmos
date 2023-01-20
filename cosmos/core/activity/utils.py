import asyncio

from typing import TYPE_CHECKING, Callable, Iterable, TypeVar

from cosmos.core.activity.tasks import async_send_activity

from . import logger

if TYPE_CHECKING:  # pragma: no cover
    from enum import Enum

    ActivityEnumType = TypeVar("ActivityEnumType", bound="Enum")


# TODO: add unittests (or functional tests) when we have an activity specific ticket
async def format_and_send_activity_in_background(
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
            if isinstance(formatter_kwargs, dict):
                payload = payload_formatter_fn(**formatter_kwargs)
            else:
                payload = (payload_formatter_fn(**instance_kwargs) for instance_kwargs in formatter_kwargs)

            await async_send_activity(payload, routing_key=activity_type.value)

        except Exception:  # noqa BLE001
            logger.exception(
                "Failed to send %s activities for with provided kwargs:\n%s", activity_type.name, formatter_kwargs
            )

    asyncio.create_task(_background_task(activity_type, payload_formatter_fn, formatter_kwargs))
