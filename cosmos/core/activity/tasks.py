import asyncio

from typing import Iterable

from cosmos_message_lib import get_connection_and_exchange, verify_payload_and_send_activity

from cosmos.core.config import settings

connection, exchange = get_connection_and_exchange(
    rabbitmq_dsn=settings.RABBITMQ_DSN,
    message_exchange_name=settings.MESSAGE_EXCHANGE_NAME,
)


async def async_send_activity(payload: dict | Iterable[dict], *, routing_key: str) -> None:
    await asyncio.to_thread(verify_payload_and_send_activity, connection, exchange, payload, routing_key)


def sync_send_activity(payload: dict | Iterable[dict], *, routing_key: str) -> None:
    verify_payload_and_send_activity(connection, exchange, payload, routing_key)
