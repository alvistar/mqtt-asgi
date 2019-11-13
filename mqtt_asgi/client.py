import signal
import socket
import time
from typing import Optional, Dict, Any

from mqtt_asgi.asyncio_helper import AsyncioHelper
from asgiref.server import StatelessServer
import asyncio
import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttClient(StatelessServer):
    count: int = 0
    # receive: asyncio.Queue
    client: mqtt.Client
    connected: asyncio.Event
    mqtt_q: 'asyncio.Queue[Dict[str, Any]]'
    should_exit: bool = False
    force_exit: bool = False
    host: str
    port: int
    mid_to_pubid: Dict[int, int] = {}

    def __init__(self, *args, client_id: str = '', host: str = 'localhost', port: int = 1883, **kwargs):
        super().__init__(*args, **kwargs)
        self.receive = asyncio.Queue()
        self.mqtt_q = asyncio.Queue()
        self.connected = asyncio.Event()

        self.client = mqtt.Client(client_id=client_id)
        self.port = port
        self.host = host
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        logger.info("Connected with result code " + str(rc))
        self.connected.set()

    def on_publish(self, client: mqtt.Client, userdata, mid):
        id = self.mid_to_pubid.get(mid)

        logger.debug(f'Received pub_ack for id: {id} and mid: {mid}')

        if id is not None:
            self.pub_ack(mid=mid, id=id)

            self.mid_to_pubid.pop(mid)

    def pub_ack(self, mid: int, id: int):
        self.mqtt_q.put_nowait({
            'type': 'mqtt_puback',
            'mid': mid,
            'id': id
        })

    def on_message(self, client: mqtt.Client, userdata, msg: mqtt.MQTTMessage):
        self.mqtt_q.put_nowait({
            'type': 'mqtt_msg',
            'payload': msg.payload,
            'topic': msg.topic})

    def on_disconnect(self, *args, **kwargs):
        logger.info('Disconnected')
        self.mqtt_q.put_nowait({
            'type': 'mqtt_disconnect',
        })
        self.connected.clear()

    async def connect(self):
        while True:
            try:
                self.client.connect(host=self.host, port=self.port)
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
        while await self.connected.wait():
            logger.info('Loop starting')
            scope = {'type': 'mqtt'}

            receive = self.get_or_create_application_instance('my_id', scope)

            receive.put_nowait({'type': 'mqtt_connect'})

            while True:
                msg = await self.mqtt_q.get()
                receive.put_nowait(msg)

                if msg['type'] == 'mqtt_disconnect':
                    break

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

    def publish(self, message):

        info: mqtt.MQTTMessageInfo = self.client.publish(
            topic=message['topic'],
            payload=message['payload'],
            retain=message['retain']
        )

        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            if info.is_published():
                self.pub_ack(id=message['id'], mid=info.mid)
            else:
                # Wait for it
                self.mid_to_pubid[info.mid] = message['id']

    async def application_send(self, scope, message):
        logger.info(f'Application sent: {message}')

        if message['type'] == 'mqtt.publish':
            self.publish(message)
        elif message['type'] == 'mqtt.subscribe':
            self.client.subscribe(
                topic=message['topic']
            )


