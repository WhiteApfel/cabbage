# -*- coding: utf-8 -*-
import asyncio
import logging

from cabbage import AmqpConnection, AsyncAmqpRpc

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


async def handler(request: str) -> str:
    n = int(request)
    logging.info(n)
    return hex(n)


async def main(rpc: AsyncAmqpRpc):
    await rpc.connect()
    await rpc.subscribe(handler, "my_queue")
    await rpc.run_server()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    connection = AmqpConnection()
    rpc = AsyncAmqpRpc(connection)
    try:
        loop.run_until_complete(main(rpc))
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(rpc.stop())
