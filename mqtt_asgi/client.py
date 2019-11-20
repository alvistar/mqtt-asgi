import socket
from typing import Optional, Dict, Any

from mqtt_asgi.asyncio_helper import AsyncioHelper
import asyncio
import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MqttClient:
    count: int = 0
    receive: asyncio.Queue
    client: mqtt.Client
    connected: asyncio.Event
    mqtt_q: 'asyncio.Queue[Dict[str, Any]]'
    should_exit: bool = False
    force_exit: bool = False
    host: str
    port: int
    mid_to_pubid: Dict[int, int] = {}
    application: Any
    restart: bool = True
    restart_delay: float = 3

    def __init__(self, application, client_id: str = '',
                 host: str = 'localhost',
                 port: int = 1883,
                 username: Optional[str] = None,
                 password: str = '',
                 restart: bool = True,
                 restart_delay: float = 3):

        self.application = application
        self.restart = restart
        self.restart_delay = restart_delay

        self.receive = asyncio.Queue()
        self.mqtt_q = asyncio.Queue()
        self.connected = asyncio.Event()

        self.client = mqtt.Client(client_id=client_id)
        self.port = port
        self.host = host

        if username:
            self.client.username_pw_set(username, password)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.client.enable_logger(logger)

    def on_connect(self, client: mqtt.Client, userdata, flags, rc):
        logger.info("Connected with result code " + str(rc))
        if rc == 0:
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

    def on_disconnect(self, client: mqtt.Client, userdata, rc):
        logger.info(f'Disconnected with rc {rc}')

        if rc == mqtt.MQTT_ERR_SUCCESS:
            exit(0)

        # Don't send messages to ASGI app
        if not self.connected.is_set():
            return

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

    async def serve(self):
        event_loop = asyncio.get_event_loop()

        AsyncioHelper(event_loop, self.client)

        await self.handle()

    def run(self):
        """
        Runs the asyncio event loop with our handler loop.
        """
        event_loop = asyncio.get_event_loop()

        AsyncioHelper(event_loop, self.client)

        try:
            event_loop.run_until_complete(self.handle())
            logger.info('exit')
        except KeyboardInterrupt:
            logger.info("Exiting due to Ctrl-C/interrupt")

    async def handle(self):
        await self.connect()

        while await self.connected.wait():
            logger.debug('Loop starting')
            scope = {'type': 'mqtt'}
            self.receive = asyncio.Queue()

            app_task = asyncio.create_task(self.application(
                scope=scope,
                receive=self.receive.get,
                send=lambda message: self.application_send(scope, message)))

            process_task = asyncio.create_task(self.process_mqtt_messages())

            (done, pending) = await asyncio.wait([app_task, process_task], return_when=asyncio.FIRST_COMPLETED)

            # Actually it will be only a task, not really a loop
            for task in done:
                if task.exception():
                    # If we have error in handling mqtt message terminate
                    if task is process_task:
                        raise task.exception()

                    # If we have error in application, log error and eventually restart session
                    if not self.restart:
                        raise task.exception()

                    logger.error(f"Exception in app: {app_task.exception()}")
                    app_task.print_stack()
                    process_task.cancel()
                    logger.info(f'Restarting app in {self.restart_delay} seconds')
                    await asyncio.sleep(self.restart_delay)

    async def process_mqtt_messages(self):
        self.receive.put_nowait({'type': 'mqtt_connect'})

        while True:
            msg = await self.mqtt_q.get()
            self.receive.put_nowait(msg)

            if msg['type'] == 'mqtt_disconnect':
                break

        self.receive.put_nowait({'type': 'mqtt_disconnect'})

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
        logger.debug(f'Application sent: {message}')

        if message['type'] == 'mqtt.publish':
            self.publish(message)
        elif message['type'] == 'mqtt.subscribe':
            self.client.subscribe(
                topic=message['topic']
            )
