# -*- coding: utf-8 -*-
import asyncio
from os import getenv

import pytest
from asynctest import MagicMock

from cabbage import AmqpConnection, AsyncAmqpRpc

pytestmark = pytest.mark.asyncio

TEST_RABBITMQ_HOST = getenv("TEST_RABBITMQ_HOST", "localhost")
TEST_VHOST = "cabbage_test"


async def test_sanity(management):
    """Basic sanity check. If this fails, something went horribly wrong (or you are not running in Docker)."""
    connection = AmqpConnection(hosts=[(TEST_RABBITMQ_HOST, 5672)])
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()
    assert (
        management.get_queue("my_queue_that_should_not_exist").get("error")
        == "Object Not Found"
    )


@pytest.mark.parametrize(
    "exchange, queue",
    [
        ("", "my_queue"),
        ("my_exchange", "my_queue"),
    ],
)
async def test_subscribe(rpc, management, exchange, queue):
    assert management.get_queue(queue).get("error") == "Object Not Found"
    await asyncio.sleep(5)  # management API seems to be super slow
    assert len(management.get_consumers()) == 1  # callback queue
    await rpc.subscribe(request_handler=lambda x: x, exchange=exchange, queue=queue)
    await asyncio.sleep(5)
    assert management.get_queue(queue).get("name") == queue
    assert len(management.get_consumers()) == 2  # callback queue + test queue


@pytest.mark.parametrize(
    "exchange, queue",
    [
        ("", "my_queue"),
        ("my_exchange", "my_queue"),
    ],
)
async def test_consume(rpc: AsyncAmqpRpc, management, exchange, queue):
    sent_data = "Test. Тест. 実験。"
    request_handler = MagicMock()
    await rpc.subscribe(request_handler=request_handler, exchange=exchange, queue=queue)
    management.publish(exchange=exchange, routing_key=queue, data=sent_data)
    await asyncio.sleep(0.1)  # give the event loop a chance to process it
    request_handler.assert_called_once_with(sent_data)


@pytest.mark.parametrize(
    "rpc_with_specified_callback_exchange",
    ["", "public", "private", "another_exchange"],
    indirect=True,
)
async def test_non_standard_exchanges(
    management, rpc_with_specified_callback_exchange: AsyncAmqpRpc
):
    sent_data = "Test. Тест. 実験。"
    exchange = rpc_with_specified_callback_exchange.callback_exchange
    queue = rpc_with_specified_callback_exchange.callback_queue
    assert exchange in [
        exchange_element["name"] for exchange_element in management.get_exchanges()
    ]
    management.publish(exchange=exchange, routing_key=queue, data=sent_data)
    await asyncio.sleep(0.1)  # give the event loop a chance to process it
    rpc_with_specified_callback_exchange._on_response.assert_called_once()
    assert rpc_with_specified_callback_exchange._on_response.call_args[0][
        1
    ] == sent_data.encode("utf-8")
