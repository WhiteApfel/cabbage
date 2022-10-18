# -*- coding: utf-8 -*-
import logging
from functools import partial
from os import getenv
from urllib.parse import quote

import pytest
import requests
from asynctest import MagicMock
from requests.auth import HTTPBasicAuth

from cabbage import AmqpConnection, AsyncAmqpRpc

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

TEST_RABBITMQ_HOST = getenv("TEST_RABBITMQ_HOST", "localhost")


class Management:
    def __init__(self, url, vhost):
        self.base_url = f"{url}/api/"
        self.vhost = vhost
        self.request_params = dict(
            auth=HTTPBasicAuth("guest", "guest"),
            headers={"content-type": "application/json"},
        )

    def _call(self, http_method, *args, **kwargs):
        args = map(partial(quote, safe=""), args)
        result = requests.request(
            http_method,
            self.base_url + "/".join(args),
            **{**self.request_params, **kwargs},
        )
        if result.text:
            return result.json()

    def get_queue(self, name):
        return self._call("get", "queues", self.vhost, name)

    def get_consumers(self):
        return self._call("get", "consumers", self.vhost)

    def get_exchanges(self):
        return self._call("get", "exchanges", self.vhost)

    def put_vhost(self):
        self._call("put", "vhosts", self.vhost)
        self._call(
            "put",
            "permissions",
            self.vhost,
            "guest",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )

    def delete_vhost(self):
        self._call("delete", "vhosts", self.vhost)

    def publish(self, exchange, routing_key, data):
        self._call(
            "post",
            "exchanges",
            self.vhost,
            exchange or "amq.default",
            "publish",
            json={
                "properties": {},
                "routing_key": routing_key,
                "payload": data,
                "payload_encoding": "string",
            },
        )


TEST_VHOST = "cabbage_test"


@pytest.fixture(scope="session")
def management():
    """Wrapper for RabbitMQ Management Plugin API."""
    return Management(f"http://{TEST_RABBITMQ_HOST}:15672", vhost=TEST_VHOST)


@pytest.yield_fixture(scope="function", autouse=True)
def vhost_environment(management: Management):
    management.put_vhost()
    yield
    management.delete_vhost()


@pytest.fixture
async def rpc(event_loop):
    """Ready-to-work RPC connected to RabbitMQ in Docker."""
    connection = AmqpConnection(
        hosts=[(TEST_RABBITMQ_HOST, 5672)], virtualhost=TEST_VHOST, loop=event_loop
    )
    rpc = AsyncAmqpRpc(connection=connection)
    await rpc.connect()
    yield rpc
    await rpc.stop()


@pytest.fixture
async def rpc_with_specified_callback_exchange(event_loop, request):
    """Ready-to-work RPC connected to RabbitMQ with specified callback exchange"""
    connection = AmqpConnection(
        hosts=[(TEST_RABBITMQ_HOST, 5672)], virtualhost=TEST_VHOST, loop=event_loop
    )
    rpc = AsyncAmqpRpc(connection=connection, callback_exchange=request.param)
    rpc._on_response = MagicMock()
    await rpc.connect()
    yield rpc
    await rpc.stop()
