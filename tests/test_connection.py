# -*- coding: utf-8 -*-
import asyncio
import ssl
from unittest import mock

import pytest
from aioamqp.protocol import CLOSED, CLOSING, CONNECTING, OPEN
from asynctest import MagicMock, patch

import cabbage
from cabbage import AmqpConnection
from cabbage.amqp import aioamqp_connect
from tests.conftest import MockProtocol, MockTransport

pytestmark = pytest.mark.asyncio

# Sample non-default connection info:
HOST = 'fake_amqp_host'
PORT = 25762
USERNAME = 'amqp_user'
PASSWORD = 'secret_password'
VIRTUALHOST = 'test_vhost'


@pytest.mark.asyncio
async def test_amqp_connection_ssl(event_loop):
    ssl_context = mock.MagicMock()
    mock_transport, mock_protocol = MockTransport(), MockProtocol()
    with patch('asyncio.base_events.BaseEventLoop.create_connection') as mock_connect:
        mock_connect.return_value = (mock_transport, mock_protocol)
        await aioamqp_connect(host=HOST, port=PORT, virtualhost=VIRTUALHOST, login=USERNAME, password=PASSWORD,
                              loop=event_loop, ssl=ssl_context)
        mock_connect.assert_called_once_with(mock.ANY, HOST, PORT, ssl=ssl_context)


@pytest.mark.asyncio
async def test_amqp_connection_exception(event_loop):
    ssl_context = mock.MagicMock()
    mock_transport, mock_protocol = MockTransport(), MockProtocol()
    mock_protocol.start_connection = mock.MagicMock(side_effect=ValueError('TestException'))
    with patch('asyncio.base_events.BaseEventLoop.create_connection') as mock_connect:
        mock_connect.return_value = (mock_transport, mock_protocol)
        with pytest.raises(ValueError, match='TestException'):
            await aioamqp_connect(host=HOST, port=PORT, virtualhost=VIRTUALHOST, login=USERNAME, password=PASSWORD,
                                  loop=event_loop, ssl=ssl_context)


@pytest.mark.asyncio
async def test_amqp_connection_ssl_context(event_loop):
    mock_transport, mock_protocol = MockTransport(), MockProtocol()
    with patch('asyncio.base_events.BaseEventLoop.create_connection') as mock_connect:
        with patch('ssl.create_default_context') as mock_default_context:
            mock_connect.return_value = (mock_transport, mock_protocol)
            await aioamqp_connect(host=HOST, port=PORT, virtualhost=VIRTUALHOST, login=USERNAME, password=PASSWORD,
                                  verify_ssl=False, ssl=mock_default_context)
            assert mock_default_context.check_hostname is False
            assert mock_default_context.verify_mode == ssl.CERT_NONE


@pytest.mark.asyncio
async def test_amqp_connection_with_ssl_port(event_loop):
    SSL_PORT = 5671
    mock_transport, mock_protocol = MockTransport(), MockProtocol()
    with patch('asyncio.base_events.BaseEventLoop.create_connection') as mock_connect:
        mock_connect.return_value = (mock_transport, mock_protocol)
        await aioamqp_connect(host=HOST, virtualhost=VIRTUALHOST, login=USERNAME, password=PASSWORD, loop=event_loop,
                              ssl=True)
        mock_connect.assert_called_once_with(mock.ANY, HOST, SSL_PORT, ssl=mock.ANY)


@pytest.mark.asyncio
async def test_amqp_connection_with_default_port(event_loop):
    DEFAULT_PORT = 5672
    mock_transport, mock_protocol = MockTransport(), MockProtocol()
    with patch('asyncio.base_events.BaseEventLoop.create_connection') as mock_connect:
        mock_connect.return_value = (mock_transport, mock_protocol)
        await aioamqp_connect(host=HOST, virtualhost=VIRTUALHOST, login=USERNAME, password=PASSWORD, loop=event_loop,
                              ssl=False)
        mock_connect.assert_called_once_with(mock.ANY, HOST, DEFAULT_PORT)


class TestConnect:
    """AmqpConnection.connect"""

    async def test_connect(self, event_loop):
        """Check typical connection call."""
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        connection = cabbage.AmqpConnection(hosts=[(HOST, PORT)], username=USERNAME, password=PASSWORD,
                                            virtualhost=VIRTUALHOST, loop=event_loop)
        with patch('cabbage.amqp.aioamqp_connect') as mock_connect:
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(host=HOST, port=PORT, login=USERNAME, password=PASSWORD,
                                             virtualhost=VIRTUALHOST, loop=event_loop, ssl=False)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_default_params(self, event_loop):
        """Check that AmqpConnection supplies reasonable defaults."""
        connection = cabbage.AmqpConnection(loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        with patch('cabbage.amqp.aioamqp_connect') as mock_connect:
            mock_connect.return_value = (mock_transport, mock_protocol)
            await connection.connect()

        mock_connect.assert_called_once_with(host='localhost', port=5672, login='guest', password='guest',
                                             virtualhost='/', loop=event_loop, ssl=False)
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_called_twice(self, event_loop):
        """Check that calling the method twice has no effect."""
        connection = cabbage.AmqpConnection(loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()
        connection.transport = mock_transport
        connection.protocol = mock_protocol
        with patch('cabbage.amqp.aioamqp_connect') as mock_connect:
            await connection.connect()

        mock_connect.assert_not_called()
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_retry(self, event_loop):
        """Keep reconnecting if an error occurs."""
        attempts = 10

        async def faulty_connect(_attempts=[attempts], **kwargs):
            assert kwargs == dict(host=HOST, port=PORT, login=USERNAME, password=PASSWORD, virtualhost=VIRTUALHOST,
                                  loop=event_loop, ssl=False)
            _attempts[0] -= 1
            if _attempts[0]:
                raise OSError('[Errno 113] Connect call failed')
            return mock_transport, mock_protocol

        connection = cabbage.AmqpConnection(hosts=[(HOST, PORT)], username=USERNAME,
                                            password=PASSWORD, virtualhost=VIRTUALHOST, loop=event_loop)
        mock_transport, mock_protocol = MockTransport(), MockProtocol()

        with patch('cabbage.amqp.aioamqp_connect', new=faulty_connect), patch('asyncio.sleep') as mock_sleep:
            await connection.connect()

        assert mock_sleep.call_count == attempts - 1
        assert connection.transport is mock_transport
        assert connection.protocol is mock_protocol

    async def test_fatal_error(self, event_loop):
        """Some connection errors are not worth trying to recover from."""
        connection = cabbage.AmqpConnection(hosts=[('angrydev.ru', 80)], loop=event_loop)

        with pytest.raises(asyncio.IncompleteReadError):
            with patch('cabbage.amqp.aioamqp_connect',
                       side_effect=asyncio.IncompleteReadError([], 160)) as mock_connect:
                await connection.connect()

        mock_connect.assert_called_once()
        assert connection.transport is None
        assert connection.protocol is None


class TestDisconnect:
    async def test_ok(self, connection):
        await connection.disconnect()
        connection.protocol.close.assert_called_once_with()

    async def test_no_protocol(self, event_loop):
        connection = cabbage.AmqpConnection(loop=event_loop)
        connection.protocol = MagicMock()
        await connection.disconnect()
        connection.protocol.close.assert_not_called()

    async def test_not_connected(self, connection):
        connection.protocol.state = CLOSED
        await connection.disconnect()
        connection.protocol.close.assert_not_called()


class TestIsConnectedProperty:
    async def test_protocol_is_none(self, connection):
        connection.protocol = None
        assert connection.is_connected is False

    @pytest.mark.parametrize('state', [CONNECTING, CLOSING, CLOSED])
    async def test_protocol_state_not_equals_open(self, connection: AmqpConnection, state):
        connection.protocol.state = state
        assert connection.is_connected is False

    async def test_ok(self, connection: AmqpConnection):
        connection.protocol = MockProtocol()
        connection.protocol.state = OPEN
        assert connection.is_connected is True
