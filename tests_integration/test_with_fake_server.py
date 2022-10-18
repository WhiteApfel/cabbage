# -*- coding: utf-8 -*-
import asyncio
import logging
from os import getenv

import pytest

import cabbage

logger = logging.getLogger(__name__)

TEST_RABBITMQ_HOST = getenv("TEST_RABBITMQ_HOST", "localhost")

pytestmark = pytest.mark.asyncio


class FakeRpcServer:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.connection = cabbage.AmqpConnection(
            hosts=[(TEST_RABBITMQ_HOST, 5672)], loop=self.loop
        )
        self.rpc = cabbage.AsyncAmqpRpc(
            connection=self.connection,
            subscriptions=[(self.handle, "fake", "", "fake")],
        )
        self.responses = {}

    async def run(self):
        await self.rpc.run()

    async def stop(self):
        await self.rpc.stop()

    async def handle(self, request: str) -> str:
        response = self.responses.get(request, None)
        logger.debug(f"FAKE RPC SERVER REQUEST: {request} RESPONSE: {response}")
        return response


async def test_ok():
    fake_rpc = FakeRpcServer()
    fake_rpc.responses["abc"] = "123"
    await fake_rpc.run()

    conn = cabbage.AmqpConnection(hosts=[(TEST_RABBITMQ_HOST, 5672)])
    rpc = cabbage.AsyncAmqpRpc(connection=conn)
    await rpc.run()
    result = await rpc.send_rpc("fake", data="abc", await_response=True)
    assert result == "123"

    await rpc.stop()
    await fake_rpc.stop()
