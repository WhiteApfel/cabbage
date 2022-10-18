# -*- coding: utf-8 -*-
import pytest

from cabbage import AmqpConnection, ServiceUnavailableError
from cabbage.test_utils import FakeAsyncAmqpRpc


def request_handler(request):
    return request


async def request_handler_async(request):
    return request


@pytest.mark.parametrize("handler", [request_handler, request_handler_async])
@pytest.mark.parametrize("body", ["text", {"type": "json"}])
@pytest.mark.asyncio
async def test_server(body, handler):
    routing_key = "test"
    connection = AmqpConnection()
    subscriptions = [(handler, routing_key)]
    rpc = FakeAsyncAmqpRpc(connection, subscriptions=subscriptions)
    await rpc.run()

    result = await rpc.fake_message(routing_key, body)
    assert result == body


@pytest.mark.asyncio
async def test_awaitable_response():
    connection = AmqpConnection()
    rpc = FakeAsyncAmqpRpc(connection)
    with pytest.raises(ValueError):
        await rpc.fake_message("invalid-queue", None)


@pytest.mark.parametrize("data", ["text", {"type": "json"}])
@pytest.mark.parametrize("expected_response", ["text", {"type": "json"}])
@pytest.mark.asyncio
async def test_client(expected_response, data):
    routing_key = "test"
    connection = AmqpConnection()
    rpc = FakeAsyncAmqpRpc(connection)
    rpc.set_response(routing_key, expected_response)
    await rpc.connect()

    response = await rpc.send_rpc(routing_key, data)
    assert response == expected_response
    assert rpc.call_args == [(routing_key, data)]

    not_awaitable = await rpc.send_rpc(routing_key, data, await_response=False)
    assert not_awaitable is None
    assert rpc.call_args == [(routing_key, data), (routing_key, data)]


@pytest.mark.asyncio
async def test_client_no_response():
    connection = AmqpConnection()
    rpc = FakeAsyncAmqpRpc(connection)
    with pytest.raises(ServiceUnavailableError):
        await rpc.send_rpc("test", None)
