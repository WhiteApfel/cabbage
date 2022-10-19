# -*- coding: utf-8 -*-
import asyncio
import inspect
import logging
import random
import socket
import ssl as ssl_module
import uuid
from asyncio import AbstractEventLoop
from functools import partial
from itertools import cycle
from typing import (
    Optional,
    Callable,
    Union,
    Awaitable,
    Mapping,
    Dict,
    Iterable,
    Literal,
    Type,
)

from aioamqp.channel import Channel
from aioamqp.envelope import Envelope
from aioamqp.properties import Properties
from aioamqp.protocol import AmqpProtocol, CONNECTING, OPEN

from .utils import FibonaccianBackoff

logger = logging.getLogger("cabbage")


class ServiceUnavailableError(Exception):
    """External service unavailable."""


class AmqpConnection:
    def __init__(
        self,
        hosts: Iterable[tuple[str, Union[str, int]]] = None,
        username: Optional[str] = "guest",
        password: Optional[str] = "guest",
        virtualhost: Optional[str] = "/",
        loop: Optional[AbstractEventLoop] = None,
        ssl: Optional[Union[bool, ssl_module.SSLContext]] = False,
    ):
        """
        :param hosts: iterable with tuples (host, port), default localhost:5672
        :param username: AMQP login, default guest
        :param password: AMQP password, default guest
        :param virtualhost: AMQP virtual host, default /
        :param loop: asyncio event loop, default current event loop
        :param ssl: bool or SSLContext, uses default ssl context if True, default False
        """
        self.loop = loop or asyncio.get_event_loop()
        self.username = username
        self.password = password
        self.virtualhost = virtualhost
        self.hosts = hosts if hosts is not None else [("localhost", 5672)]
        self._connection_cycle = self.cycle_hosts()
        self.transport = None
        self.protocol = None
        self.ssl = ssl

    async def channel(self) -> Channel:
        if self.protocol is not None:
            return await self.protocol.channel()

    def cycle_hosts(self, shuffle=False):
        if shuffle:
            random.shuffle(self.hosts)
        yield from cycle(self.hosts)

    async def connect(self):
        """Connect to AMQP broker. On failure this function will endlessly try reconnecting.
        Do nothing if already connected or connecting.
        """
        if self.protocol is not None and self.protocol.state in [CONNECTING, OPEN]:
            return

        delay = FibonaccianBackoff(limit=60.0)
        for host, port in self._connection_cycle:
            try:
                self.transport, self.protocol = await aioamqp_connect(
                    loop=self.loop,
                    host=host,
                    port=port,
                    login=self.username,
                    password=self.password,
                    virtualhost=self.virtualhost,
                    ssl=self.ssl,
                )
            except OSError as e:
                # Connection-related errors are mostly represented by `ConnectionError`,
                # except for some like `socket.gaierror`, which fall into broader `OSError`
                next_delay = delay.next()
                logger.warning(
                    f"failed to connect to {host}:{port}, error <{e.__class__.__name__}> {e}, "
                    f"retrying in {next_delay} seconds"
                )
                await asyncio.sleep(next_delay)
            except Exception as e:
                logger.error(
                    f"connection failed, not retrying: <{e.__class__.__name__}> {e}"
                )
                raise
            else:
                logger.info(f"wait connection to {host}:{port}")
                break

    async def disconnect(self):
        if self.protocol is not None and self.protocol.state in [CONNECTING, OPEN]:
            await self.protocol.close()

    @property
    def is_connected(self) -> bool:
        """Property, required for rpc to check readiness"""
        return bool(self.protocol) and self.protocol.state == OPEN


class AsyncAmqpRpc:
    def __init__(
        self,
        connection: AmqpConnection,
        exchange_params: Mapping = None,
        queue_params: Mapping = None,
        subscriptions=None,
        prefetch_count=1,
        raw=False,
        default_response_timeout=15.0,
        shutdown_timeout=60.0,
        connection_delay: float = 0.1,
        callback_exchange="",
    ):
        """
        All arguments are optional. If `request_handler` is not supplied or None, RPC works only in client mode.

        :param queue_params: options for creating queues, default durable and DLX
        :param exchange_params: options when creating exchanges, default durable and type topic
        :param subscriptions: list of tuples (handler, queue, exchange, routing_key, queue_params, exchange_params)
                Rightmost parameters are optional, you can specify only (handler, queue).
        :param raw: do not attempt decoding, use iff `request_handler` maps `bytes -> bytes`.
        :param prefetch_count: per-consumer prefetch message limit, default 1
        :param default_response_timeout: default timeout for awaiting response when sending remote calls
        :param shutdown_timeout: timeout for handlers to finish gracefully on shutdown
        """
        self.raw = raw
        self.queue_params = queue_params
        self.exchange_params = exchange_params
        self.start_subscriptions = subscriptions or []
        self.default_response_timeout = default_response_timeout
        self.shutdown_timeout = shutdown_timeout
        self.connection = connection
        self.prefetch_count = prefetch_count
        self.keep_running = True
        self.channel = None
        self.callback_queue = None
        self.callback_exchange = callback_exchange
        self._responses: Dict[str, asyncio.Future] = {}
        self._tasks = set()
        self._subscriptions = set()
        self.connection_delay = connection_delay

    async def connect(self):
        await self.connection.connect()
        self.channel = await self.connection.channel()

        result = await self.channel.queue_declare(exclusive=True)
        self.callback_queue = result["queue"]

        # setup client
        if self.callback_exchange != "":
            # operation not permitted on default exchange
            await self.channel.exchange_declare(
                exchange_name=self.callback_exchange, type_name="topic", durable=True
            )
            await self.channel.queue_bind(
                queue_name=self.callback_queue,
                exchange_name=self.callback_exchange,
                routing_key=self.callback_queue,
            )

        await self.channel.basic_consume(
            callback=self._on_response,
            queue_name=self.callback_queue,
        )

        logger.debug(f'listening on callback queue {result["queue"]}')

    # AMQP server implementation

    async def declare(
        self,
        queue: str,
        exchange: str = "",
        routing_key: str = None,
        queue_params: Mapping = None,
        exchange_params: Mapping = None,
    ):
        """
        Set up necessary objects — exchange, queue, binding, QoS.

        :param queue: queue name
        :param exchange: exchange name, default '' (default AMQP exchange)
        :param routing_key: routing key, default same as `queue`
        :param queue_params: options for the queue, default durable and DLX
        :param exchange_params: options for the exchange, default durable and type topic
        """
        if routing_key is None:
            routing_key = queue
        if exchange_params is None:
            exchange_params = dict(type_name="topic", durable=True)
        if queue_params is None:
            queue_params = dict(
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "DLX",
                    "x-dead-letter-routing-key": "dlx_rpc",
                },
            )
        if exchange != "":
            # operation not permitted on default exchange
            await self.channel.exchange_declare(
                exchange_name=exchange, **exchange_params
            )
        result = await self.channel.queue_declare(queue_name=queue, **queue_params)
        queue = result["queue"]  # in case queue name was generated by broker
        if exchange != "":
            # operation not permitted on default exchange
            await self.channel.queue_bind(
                queue_name=queue, exchange_name=exchange, routing_key=routing_key
            )
        await self.channel.basic_qos(
            prefetch_count=self.prefetch_count,
            prefetch_size=0,
            connection_global=False,
        )

    async def subscribe(
        self,
        request_handler: Union[
            Callable[[str], Optional[str]],
            Callable[[bytes], Optional[bytes]],
            Callable[[str], Awaitable[Optional[str]]],
            Callable[[bytes], Awaitable[Optional[bytes]]],
        ],
        queue: str,
        exchange: str = "",
        routing_key: str = None,
        resubscribe: bool = False,
    ) -> str:
        """
        Subscribe to a specific queue. Exchange and queue will be created if they do not exist.

        :param request_handler: request handler, can be a normal or coroutine function
                that maps either str->str or bytes->bytes. If `request_handler` returns None,
                it is taken to mean no response is needed.
        :param exchange: exchange name, default '' (default AMQP exchange)
        :param queue: queue name
        :param routing_key: routing key, default same as `queue`
        :param resubscribe: resubscribe on reconnect
        :return: consumer_tag
        """
        if routing_key is None:
            routing_key = queue
        if resubscribe:
            self.start_subscriptions.append(
                (request_handler, queue, exchange, routing_key)
            )
        await self.declare(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            queue_params=self.queue_params,
            exchange_params=self.exchange_params,
        )
        result = await self.channel.basic_consume(
            callback=partial(self._on_request, request_handler=request_handler),
            queue_name=queue,
        )
        consumer_tag = result["consumer_tag"]
        self._subscriptions.add(consumer_tag)
        logger.debug(
            f"subscribed to queue {queue}, bound to exchange {exchange} with key {routing_key} "
            f"(consumer tag {consumer_tag})"
        )
        return consumer_tag

    async def unsubscribe(self, consumer_tag: str):
        """
        Stop consuming on a queue.

        :param consumer_tag: consumer tag returned by `subscribe()`
        """
        logger.debug(f"unsubscribed from a queue (consumer tag {consumer_tag})")
        await self.channel.basic_cancel(consumer_tag=consumer_tag)

    async def _on_request(self, channel, body, envelope, properties, request_handler):
        """Run handle_rpc() in background."""
        task = asyncio.ensure_future(
            self.handle_rpc(channel, body, envelope, properties, request_handler)
        )
        self._tasks.add(task)
        task.add_done_callback(lambda fut: self._tasks.remove(fut))

    async def handle_rpc(
        self,
        channel: Channel,
        body: bytes,
        envelope: Envelope,
        properties: Properties,
        request_handler,
    ):
        """Process request with handler and send response if needed."""
        try:
            data = body if self.raw else body.decode("utf-8")
            logger.debug(
                f"> handle_rpc: data {data}, routing_key {properties.reply_to}, "
                f"correlation_id {properties.correlation_id}"
            )
            response = request_handler(data)
            if inspect.isawaitable(response):
                response = await response
        except Exception as e:
            logger.error(
                f"handle_rpc. error <{e.__class__.__name__}> {e}, routing_key {properties.reply_to}, "
                f"correlation_id {properties.correlation_id}"
            )
            await channel.basic_client_nack(delivery_tag=envelope.delivery_tag)
        else:
            responding = properties.reply_to is not None and response is not None
            logger.debug(
                f'{"< " * responding}handle_rpc: responding? {responding}, routing_key {properties.reply_to}, '
                f"correlation_id {properties.correlation_id}, result {response}"
            )
            if responding:
                response_params = dict(
                    payload=response if self.raw else response.encode("utf-8"),
                    exchange_name="",
                    routing_key=properties.reply_to,
                )

                if properties.correlation_id:
                    response_params["properties"] = {
                        "correlation_id": properties.correlation_id
                    }

                await channel.basic_publish(**response_params)

            if channel.is_open:
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def run_server(self):
        """Main routine for the server."""
        try:
            while self.keep_running:
                await self.connect()
                # TODO: reconnect to manual subscriptions on lost connection
                for params in self.start_subscriptions:
                    await self.subscribe(*params)
                await self.connection.protocol.wait_closed()
        finally:
            await self.connection.disconnect()

    async def run(self, app=None):
        """aiohttp-compatible on_startup coroutine."""
        asyncio.ensure_future(self.run_server())
        await self.wait_connected()
        logger.info("Waiting finished. Connected successfully.")

    async def stop(self, app=None):
        """aiohttp-compatible on_shutdown coroutine."""
        for consumer_tag in self._subscriptions:
            await self.unsubscribe(consumer_tag)
        if self._tasks:
            logger.info(f"waiting for {len(self._tasks)} task(s) to finish normally")
            done, pending = await asyncio.wait(
                self._tasks, timeout=self.shutdown_timeout
            )
            if pending:
                level = logger.warning
            else:
                level = logger.info
            level(
                f"{len(done)} task(s) finished, {len(pending)} task(s) did not finish in time"
            )
        self.keep_running = False
        await self.connection.disconnect()

    # AMQP client implementation

    async def send_rpc(
        self,
        destination: str,
        data: Union[str, bytes],
        exchange: str = "",
        await_response=True,
        timeout: float = None,
        correlation_id: str = None,
    ) -> Union[str, bytes, None]:
        """
        Execute a method on remote server. Sends `data` to `destination` routing key.

        If `await_response` is True, the call blocks coroutine until the result is returned or until `timeout` seconds
        passed (class default is used if None). AMQP correlation_id is set to `correlation_id` or new UUID if None.

        Raises `ServiceUnavailableError` on response timeout.
        """
        # TODO: retry on error?
        if isinstance(data, str):
            payload = data.encode("utf-8")
            raw = False
        else:
            payload = data
            raw = True
        properties = dict()
        if await_response:
            if correlation_id is None:
                correlation_id = str(uuid.uuid4())
            properties = {
                "reply_to": self.callback_queue,
                "correlation_id": correlation_id,
            }
        logger.debug(
            f"< send_rpc: destination {destination}, data {data}, "
            f"awaiting? {await_response}, timeout {timeout}, properties {properties}"
        )
        await self.channel.basic_publish(
            exchange_name=exchange,
            routing_key=destination,
            properties=properties,
            payload=payload,
        )

        if await_response:
            if timeout is None:
                timeout = self.default_response_timeout
            response = await self._await_response(
                correlation_id=correlation_id, timeout=timeout
            )
            if not raw:
                response = response.decode("utf-8")
            logger.debug(f"> send_rpc: response {response}")
            return response

    async def _await_response(self, correlation_id, timeout):
        """Wait for a response with given correlation id. Blocks current Task."""
        self._responses[correlation_id] = asyncio.Future()
        try:
            await asyncio.wait_for(self._responses[correlation_id], timeout=timeout)
            return self._responses[correlation_id].result()
        except asyncio.TimeoutError:
            logger.warning(f"request {correlation_id} timed out")
            raise ServiceUnavailableError("Request timed out") from None
        finally:
            self._responses.pop(correlation_id)

    async def _on_response(
        self, channel: Channel, body: bytes, envelope: Envelope, properties: Properties
    ):
        """Set response result. Called by aioamqp on a message in callback queue."""
        if properties.correlation_id in self._responses:
            self._responses[properties.correlation_id].set_result(body)
        else:
            logger.warning(
                f"unexpected message with correlation_id {properties.correlation_id}."
            )

        if channel.is_open:
            await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)

    async def wait_connected(self):
        while not all([self.connection.is_connected, self.channel]):
            logger.debug(f"Waiting connection for {self.connection_delay}s...")
            await asyncio.sleep(self.connection_delay)


async def aioamqp_connect(
    host: str = "localhost",
    port: int = None,
    login: Optional[str] = "guest",
    password: Optional[str] = "guest",
    virtualhost: Optional[str] = "/",
    ssl: Optional[Union[bool, ssl_module.SSLContext]] = False,
    login_method: Literal[
        "PLAIN", "AMQPLAIN", "RABBIT-CR-DEMO", "EXTERNAL"
    ] = "AMQPLAIN",
    insist: Optional[bool] = False,
    protocol_factory: Type[AmqpProtocol] = AmqpProtocol,
    *,
    verify_ssl: Optional[bool] = True,
    loop: Optional[AbstractEventLoop] = None,
    timeout: Optional[Union[int, float]] = None,
    **kwargs,
):
    """Convenient method to connect to an AMQP broker
    :param host:          the host to connect to
    :param port:          broker port
    :param login:         login
    :param password:      password
    :param virtualhost:   AMQP virtualhost to use for this connection
    :param ssl:           bool: Create an SSL connection instead of a plain unencrypted one
                          SSLContext: Set custom SSLContext object
    :param verify_ssl:    Verify server's SSL certificate (True by default)
    :param login_method:  AMQP auth method
    :param insist:        Insist on connecting to a server
    :param protocol_factory: Factory to use, if you need to subclass AmqpProtocol
    :param loop:          Set the event loop to use
    :param timeout:       Connection close timeout after exception
    :param kwargs:        Arguments to be given to the protocol_factory instance
    :return:              a tuple (transport, protocol) of an AmqpProtocol instance
    """
    SSL_PORT = 5671
    DEFAULT_PORT = 5672

    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        return protocol_factory(loop=loop, **kwargs)

    create_connection_kwargs = {}

    if ssl:
        ssl_context = (
            ssl_module.create_default_context() if isinstance(ssl, bool) else ssl
        )
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE
        create_connection_kwargs["ssl"] = ssl_context

    if port is None:
        if ssl:
            port = SSL_PORT
        else:
            port = DEFAULT_PORT

    transport, protocol = await loop.create_connection(
        factory, host, port, **create_connection_kwargs
    )

    # these 2 flags *may* show up in sock.type. They are only available on linux
    # see https://bugs.python.org/issue21327
    nonblock = getattr(socket, "SOCK_NONBLOCK", 0)
    cloexec = getattr(socket, "SOCK_CLOEXEC", 0)
    sock = transport.get_extra_info("socket")
    if sock is not None and (sock.type & ~nonblock & ~cloexec) == socket.SOCK_STREAM:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    try:
        await protocol.start_connection(
            host,
            port,
            login,
            password,
            virtualhost,
            ssl=ssl,
            login_method=login_method,
            insist=insist,
        )
    except Exception:
        await protocol.wait_closed(timeout=timeout)
        raise

    return transport, protocol
