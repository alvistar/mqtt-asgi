from dispatcher import Dispatcher
from mqtt_asgi import MqttServer
from typing import Callable, Any, NewType, Awaitable

import logging

logger = logging.getLogger()

Send = Callable[[Any], None]
Receive = Callable[[], Awaitable]


class Session:
    send: Send
    receive: Receive
    connected: bool

    def __init__(self, send: Send, receive: Receive):
        self.send = send
        self.receive = receive
        self.connected = True

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

            print(msg)
            await action(self, msg['topic'], msg['payload'])

    async def on_connect(self):
        await self.subscribe('configure/#')

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


class App:
    dispatcher: Dispatcher
    session: Session

    async def __call__(self, scope, receive, send):
        self.scope = scope
        self.receive = receive
        self.send = send
        self.session = Session(send, receive)

        await self.session.main()

    def __init__(self):
        logger.info('Initialized')
        self.dispatcher = Dispatcher()


app = App()


@app.dispatcher.topic('test/+')
async def action(session: Session, topic: str, payload: bytes):
    print(f'with topic {topic} and payload: {payload}')
    await session.publish('hello', b'world')
    await session.publish('ciao', b'mondo')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    server = MqttServer(app)
    server.run()
