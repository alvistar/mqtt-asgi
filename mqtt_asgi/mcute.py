from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .dispatcher import Action, dispatch
from typing import Callable, Any, Awaitable, List, Optional

import logging

logger = logging.getLogger()

Send = Callable[[Any], None]
Receive = Callable[[], Awaitable]


@dataclass
class MqttMessage:
    topic: str
    payload: bytes
    retain: bool = False


class Session:
    send: Send
    receive: Receive
    connected: bool
    registry: List[Action]
    on_connect_cb: Callable

    def __init__(self, send: Send, receive: Receive, registry: List[Action], on_connect_cb: Callable = None):
        self.send = send
        self.receive = receive
        self.connected = True
        self.registry = registry
        self.on_connect_cb = on_connect_cb

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
        if self.on_connect_cb:
            await self.on_connect_cb()

        for action in self.registry:
            if action.subscribe:
                await self.subscribe(action.topic)

    async def on_disconnect(self):
        logger.info('Disconnected!')
        pass

    async def publish(self, topic: str, payload: bytes, retain: bool = False):
        await self.send( {
            'type': 'mqtt.publish',
            'topic': topic,
            'payload': payload,
            'retain': retain
        })

    async def subscribe(self, topic: str):
        await self.send( {
            'type': 'mqtt.subscribe',
            'topic': topic,
        })

    async def dispatch(self, topic: str, **kwargs):
        action, args = dispatch(topic, self.registry)
        await action.callback(*args, **kwargs)


class Mcute:
    session: Optional[Session]
    registry: List[Action] = []
    msg_q: List[MqttMessage] = []
    on_connect: Callable = None

    async def __call__(self, scope, receive, send):
        self.scope = scope
        self.session = Session(send, receive, self.registry, self.on_connect)

        # Dequeu messages

        while len(self.msg_q) > 0:
            msg = self.msg_q.pop(0)
            await self.session.publish(msg.topic, msg.payload)

        await self.session.main()
        self.session = None

    def __init__(self):
        logger.info('Initialized')

    def register(self, topic: str, callback: Callable, subscribe: bool = True):
        self.registry.append(Action(
            topic=topic,
            callback=callback,
            subscribe=subscribe
        ))

    def action(self, topic: str, subscribe: bool = True):
        def decorator(f: Callable):
            self.register(topic=topic, callback=f, subscribe=subscribe)
            return f

        return decorator

    async def publish(self, topic: str, payload: bytes, retain: bool= False):
        if self.session:
            await self.session.publish(topic=topic, payload=payload, retain=retain)
        else:
            self.msg_q.append(MqttMessage(topic=topic, payload=payload, retain=retain))

