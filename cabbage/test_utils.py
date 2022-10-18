# -*- coding: utf-8 -*-
import inspect

from cabbage.amqp import AsyncAmqpRpc, ServiceUnavailableError


class FakeAsyncAmqpRpc(AsyncAmqpRpc):
    """
    Can be used instead of AsyncAmqpRpc class for tests.
    Allows to set expected responses for testing amqp rpc client and to send fake message for testing a server.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subscriptions = {}
        self.responses = {}
        self.call_args = []

    async def connect(self):
        pass

    def set_response(self, routing_key, data):
        self.responses[routing_key] = data

    async def send_rpc(
        self,
        destination,
        data,
        exchange="",
        await_response=True,
        timeout=None,
        correlation_id=None,
    ):
        self.call_args.append((destination, data))
        if not await_response:
            return

        response = self.responses.get(destination)

        if response is None:
            raise ServiceUnavailableError()

        return response

    async def run_server(self):
        for request_handler, queue, *_ in self.start_subscriptions:
            self.subscriptions[queue] = request_handler

    async def run(self, app=None):
        await self.run_server()

    async def fake_message(self, queue, data):
        if queue not in self.subscriptions:
            raise ValueError(f"invalid queue name {queue}")

        request_handler = self.subscriptions[queue]
        response = request_handler(data)

        if inspect.isawaitable(response):
            response = await response

        return response
