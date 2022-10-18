# -*- coding: utf-8 -*-
import asyncio
import logging

import pytest
from asynctest import MagicMock
import aioamqp

import cabbage

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


@pytest.fixture
def connection(event_loop):
    conn = cabbage.AmqpConnection(hosts=[(HOST, 5672)], loop=event_loop)
    conn.transport = MockTransport()
    conn.protocol = MockProtocol()
    return conn


@pytest.fixture
def rpc(connection):
    _rpc = cabbage.AsyncAmqpRpc(connection=connection)
    loop = asyncio.new_event_loop()
    loop.run_until_complete(_rpc.connect())
    return _rpc


# some non-default values to use in tests


HOST = "fake_amqp_host"
TEST_EXCHANGE = "rpc_exchange"
TEST_DESTINATION = "rpc_destination"
SUBSCRIPTION_QUEUE = "rpc_subscription_queue"
RANDOM_QUEUE = "amq.gen-random_queue_name"
SUBSCRIPTION_KEY = "rpc_subscription_key"
RESPONSE_CORR_ID = "response_correlation_id"
CONSUMER_TAG = "some_consumer_tag"
DELIVERY_TAG = 10

# aioamqp classes mocked as factory functions:


def MockTransport():
    return MagicMock(name="MockTransport")


def MockProtocol():
    m = MagicMock(spec=aioamqp.protocol.AmqpProtocol, name="MockProtocol")
    m.state = aioamqp.protocol.OPEN
    m.channel.return_value = MockChannel()
    return m


def MockChannel():
    m = MagicMock(spec=aioamqp.channel.Channel, name="MockChannel")
    m.queue_declare.side_effect = lambda queue_name="", *a, **kw: {
        "queue": queue_name or RANDOM_QUEUE
    }
    m.basic_consume.return_value = {"consumer_tag": CONSUMER_TAG}
    return m


def MockEnvelope():
    m = MagicMock(spec=aioamqp.envelope.Envelope, name="MockEnvelope")
    m.delivery_tag = DELIVERY_TAG
    return m


def MockProperties():
    m = MagicMock(spec=aioamqp.properties.Properties, name="MockProperties")
    m.correlation_id = RESPONSE_CORR_ID
    m.reply_to = RANDOM_QUEUE
    return m
