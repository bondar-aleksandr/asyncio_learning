import asyncio
import logging
import random
import string
import uuid
import signal
import sys

if sys.platform == 'win32':
    signals = None
else:
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s,%(msecs)d %(levelname)s: %(message)s',
    datefmt='%H:%M:%S',
)

class PubSubMessage:
    def __init__(self, instance_name, message_id):
        self.instance_name = instance_name
        self.message_id = message_id
        self.hostname = f'{self.instance_name}.example.net'
        self.restarted: bool = False
        self.saved: bool = False
        self.acked: bool = False
        self.extended_cnt: int = 0


    def __str__(self):
        return f'PubSubMessage(instance name: {self.instance_name}, extended_cnt: {self.extended_cnt})'


async def publish(queue: asyncio.Queue):
    choices = string.ascii_lowercase + string.digits

    while True:
        host_id = ''.join(random.choices(choices, k=4))
        instance_name = f'cattle-{host_id}'
        msg_id = str(uuid.uuid4())
        msg = PubSubMessage(message_id=msg_id, instance_name=f'{instance_name}')
        asyncio.create_task(queue.put(msg))
        logging.debug(f'Published messages {msg}')
        await asyncio.sleep(random.random())


async def consume(queue: asyncio.Queue):
    while True:
        msg = await queue.get()
        if random.randrange(1,5) == 3:
            raise Exception(f'Could not consume message {msg}')
        logging.info(f'Consumed {msg}')
        asyncio.create_task(handle_message(msg))
        await asyncio.sleep(random.random())


async def cleanup(msg: PubSubMessage, event: asyncio.Event):
    await event.wait()
    await asyncio.sleep(random.random())
    msg.acked = True
    logging.info(f'Done. Message {msg} acknowledged')


async def handle_message(msg: PubSubMessage):
    event = asyncio.Event()
    asyncio.create_task(cleanup(msg, event))
    asyncio.create_task(extend(msg, event))
    await asyncio.gather(restart_host(msg), save(msg))
    event.set()


async def extend(msg: PubSubMessage, event: asyncio.Event):
    while not event.is_set():
        msg.extended_cnt += 1
        logging.info(f'Extended deadline for 3 sec for message: {msg}')
        await asyncio.sleep(2)


async def restart_host(msg: PubSubMessage):
    await asyncio.sleep(random.random())
    msg.restarted = True
    logging.info(f'Restarted host {msg.hostname}')


async def save(msg: PubSubMessage):
    await asyncio.sleep(random.random())
    msg.saved = True
    logging.info(f'Saved message {msg} in database!')


async def shutdown(loop, s: signal = None):
    if s:
        logging.info(f'Received exit signal {s.name}')
    logging.info('Closing database connection...')
    tasks = [t for t in asyncio.all_tasks() if not asyncio.current_task]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    loop.stop()


def handle_exception(loop, context):
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop=loop))


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(handle_exception)

    if signals:
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(loop=loop, s=s)))

    loop.create_task(publish(queue))
    loop.create_task(consume(queue))
    loop.run_forever()

if __name__ == '__main__':
    main()