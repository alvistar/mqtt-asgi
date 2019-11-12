from __future__ import annotations

import re
from dataclasses import dataclass
import dispatcher

from mqtt_asgi import MqttServer
from typing import Callable, Any, NewType, Awaitable, List

import logging

logger = logging.getLogger()

Send = Callable[[Any], None]
Receive = Callable[[], Awaitable]


class Session:
    send: Send
    receive: Receive
    connected: bool
    registry: List[Action]

    def __init__(self, send: Send, receive: Receive, registry: List[Action]):
        self.send = send
        self.receive = receive
        self.connected = True
        self.registry = registry


    async def main(self):
        msg = await self.receive()

        assert msg['type'] == 'mqtt_connect'
        await self.on_connect()

        logger.info(msg)

        while True:
            msg = await self.receive()
            if msg['type'] == 'mqtt_disconnect':
                await self.on_disconnect()
                break

            await self.dispatch(topic=msg['topic'], session=self, payload=msg['payload'])

    async def on_connect(self):
        for action in self.registry:
            if action.subscribe:
                await self.subscribe(action.topic)

    async def on_disconnect(self):
        logger.info('Disconnected!')
        pass

    async def publish(self, topic: str, payload: bytes):
        await self.send( {
            'type': 'mqtt.publish',
            'topic': topic,
            'payload': payload
        })

    async def subscribe(self, topic: str):
        await self.send( {
            'type': 'mqtt.subscribe',
            'topic': topic,
        })

    async def dispatch(self, topic: str, **kwargs):
        action, args = dispatcher.dispatch(topic, self.registry)
        await action.callback(*args, **kwargs)


class App:
    session: Session
    registry: List[dispatcher.Action] = []

    async def __call__(self, scope, receive, send):
        self.scope = scope
        self.receive = receive
        self.send = send
        self.session = Session(send, receive, self.registry)

        await self.session.main()

    def __init__(self):
        logger.info('Initialized')

    def register(self, topic: str, callback: Callable, subscribe: bool = True):
        self.registry.append(dispatcher.Action(
            topic=topic,
            callback=callback,
            subscribe=subscribe
        ))

app = App()


async def my_action(*args, session: Session, payload: bytes):
    print(f'with args {args} and payload: {payload}')
    await session.publish('hello', b'world')
    await session.publish('ciao', b'mondo')

app.register(topic='configure/+', callback=my_action)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    server = MqttServer(app)
    server.run()
