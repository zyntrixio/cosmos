import asyncio

from collections.abc import Iterable

from cosmos_message_lib import get_connection_and_exchange, verify_payload_and_send_activity

from cosmos.core.config import core_settings

connection, exchange = get_connection_and_exchange(
    rabbitmq_dsn=core_settings.RABBITMQ_DSN,
    message_exchange_name=core_settings.MESSAGE_EXCHANGE_NAME,
)


async def async_send_activity(payload: dict | Iterable[dict], *, routing_key: str) -> None:
    await asyncio.to_thread(verify_payload_and_send_activity, connection, exchange, payload, routing_key)


def sync_send_activity(payload: dict | Iterable[dict], *, routing_key: str) -> None:
    verify_payload_and_send_activity(connection, exchange, payload, routing_key)
