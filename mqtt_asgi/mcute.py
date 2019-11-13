from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .dispatcher import Action, dispatch
from typing import Callable, Any, Awaitable, List, Optional, Dict

import logging

logger = logging.getLogger()

Send = Callable[[Any], None]
Receive = Callable[[], Awaitable]


@dataclass
class MqttMessage:
    topic: str
    payload: bytes
    retain: bool = False


class Mcute:
    registry: List[Action] = []
    msg_q: List[MqttMessage] = []
    on_connect_cb: Callable = None
    receive: Optional[Callable[[], Awaitable]]
    send: Optional[Callable[[Dict[str, Any]], None]]
    pub_id: int = 0
    pub_acks: Dict[id, asyncio.Event] = {}

    async def __call__(self, scope, receive, send):
        self.scope = scope
        self.receive = receive
        self.send = send

        await self.main()
        self.send = None

    def __init__(self):
        logger.info('Initialized')

    async def main(self):
        #Get connection msg
        msg = await self.receive()

        assert msg['type'] == 'mqtt_connect'
        await self.on_connect()

        # Dequeu messages

        while len(self.msg_q) > 0:
            msg = self.msg_q.pop(0)
            await self.publish(msg.topic, msg.payload)

        while True:
            msg = await self.receive()

            if msg['type'] == 'mqtt_disconnect':
                await self.on_disconnect()
                break
            elif msg['type'] == 'mqtt_msg':
                await self.dispatch(topic=msg['topic'], instance=self, payload=msg['payload'])
            elif msg['type'] == 'mqtt_puback':
                self.on_publish(msg)

    def on_publish(self, message: Dict[str, any]):
        logger.debug(f"Got ack for {message['id']}")
        event = self.pub_acks.get(message['id'])
        if event:
            event.set()
            self.pub_acks.pop(message['id'])

    async def on_connect(self):
        if self.on_connect_cb:
            await self.on_connect_cb()

        for action in self.registry:
            if action.subscribe:
                await self.subscribe(action.topic)

    async def on_disconnect(self):
        logger.info('Disconnected!')
        pass

    async def dispatch(self, topic: str, **kwargs):
        action, args = dispatch(topic, self.registry)
        await action.callback(*args, **kwargs)

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
        if self.send:
            await self.send({
                'type': 'mqtt.publish',
                'topic': topic,
                'payload': payload,
                'retain': retain,
                'id': self.pub_id
            })
            self.pub_id = self.pub_id + 1
        else:
            self.msg_q.append(MqttMessage(topic=topic, payload=payload, retain=retain))

    async def publish_wait(self, *args, **kwargs):
        event = asyncio.Event()
        self.pub_acks[self.pub_id] = event
        logger.debug(f'Wating for message with id {self.pub_id}')
        await self.publish(*args, **kwargs)
        await event.wait()

    async def subscribe(self, topic: str):
        await self.send( {
            'type': 'mqtt.subscribe',
            'topic': topic,
        })

