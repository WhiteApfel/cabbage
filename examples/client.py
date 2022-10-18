# -*- coding: utf-8 -*-
import asyncio
import logging
import random

from cabbage import AmqpConnection, AsyncAmqpRpc

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


async def main(rpc):
    await rpc.connect()

    while True:
        n = random.randint(1, 100)
        hex_n = await rpc.send_rpc("my_queue", str(n))
        logging.info("request: {}, response: {}".format(n, hex_n))
        await asyncio.sleep(1)


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
