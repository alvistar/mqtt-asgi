import signal
import socket
import time
from typing import Optional

from mqtt_asgi.asyncio_helper import AsyncioHelper
from asgiref.server import StatelessServer
import asyncio
import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttServer(StatelessServer):
    count: int = 0
    # receive: asyncio.Queue
    client: mqtt.Client
    connected: asyncio.Event
    mqtt_q: 'Optional[asyncio.Queue[mqtt.MQTTMessage]]'
    should_exit: bool = False
    force_exit: bool = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.receive = asyncio.Queue()
        self.mqtt_q = asyncio.Queue()
        self.connected = asyncio.Event()

        self.client = mqtt.Client(client_id="asgi")
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        logger.info("Connected with result code " + str(rc))
        self.connected.set()

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        try:
            self.mqtt_q.put_nowait(msg)
            logger.info('New msg')
        except Exception as e:
            logger.error(e)

    def on_disconnect(self, *args, **kwargs):
        logger.info('Disconnected')
        self.mqtt_q.put_nowait(None)
        self.connected.clear()

    async def connect(self):
        while True:
            try:
                self.client.connect('localhost')
                break
            except (socket.error, OSError, mqtt.WebsocketConnectionError):
                logger.info('Retrying connection')
                await asyncio.sleep(3)

        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        await self.handle()

    async def serve(self):
        event_loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.application_checker())

        AsyncioHelper(event_loop, self.client)

        await self.connect()

    def run(self):
        """
        Runs the asyncio event loop with our handler loop.
        """
        event_loop = asyncio.get_event_loop()
        asyncio.ensure_future(self.application_checker())

        AsyncioHelper(event_loop, self.client)

        try:
            event_loop.run_until_complete(self.connect())
        except KeyboardInterrupt:
            logger.info("Exiting due to Ctrl-C/interrupt")

    async def handle(self):
        task = None
        while await self.connected.wait():
            logger.info('Loop starting')
            scope = {'type': 'mqtt'}

            receive = self.get_or_create_application_instance('my_id', scope)

            receive.put_nowait({'type': 'mqtt_connect'})

            while True:
                msg = await self.mqtt_q.get()
                if msg is None:
                    break
                receive.put_nowait({
                    'type': 'mqtt',
                    'payload': msg.payload,
                    'topic': msg.topic})

            receive.put_nowait({'type': 'mqtt_disconnect'})

    def get_or_create_application_instance(self, scope_id, scope):
        """
        Creates an application instance and returns its queue.
        """
        if scope_id in self.application_instances:
            self.application_instances[scope_id]["last_used"] = time.time()
            return self.application_instances[scope_id]["input_queue"]
        # See if we need to delete an old one
        while len(self.application_instances) > self.max_applications:
            self.delete_oldest_application_instance()
        # Make an instance of the application
        input_queue = asyncio.Queue()

        # application_instance = self.application(scope=scope)
        # Run it, and stash the future for later checking
        future = asyncio.ensure_future(
            self.application(
                scope=scope,
                receive=input_queue.get,
                send=lambda message: self.application_send(scope, message),
            )
        )
        self.application_instances[scope_id] = {
            "input_queue": input_queue,
            "future": future,
            "scope": scope,
            "last_used": time.time(),
        }
        return input_queue

    async def application_send(self, scope, message):
        logger.info(f'Application sent: {message}')

        if message['type'] == 'mqtt.publish':
            self.client.publish(
                topic=message['topic'],
                payload=message['payload'],
                retain=message['retain']
            )
        elif message['type'] == 'mqtt.subscribe':
            self.client.subscribe(
                topic=message['topic']
            )


