import logging
import asyncio
from paho.mqtt.client import WebsocketConnectionError
import paho.mqtt.client as mqtt

import socket

logger = logging.getLogger('asyncio-helper')


class AsyncioHelper:
    misc: asyncio.Task

    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        logger.debug("Socket opened")

        def cb():
            logger.debug("Socket is readable, calling loop_read")
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        logger.debug("Socket closed")
        self.loop.remove_reader(sock)
        # self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        logger.debug("Watching socket for writability.")

        def cb():
            logger.debug("Socket is writable, calling loop_write")
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        logger.debug("Stop watching socket for writability.")
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        logger.debug("misc_loop started")

        while True:
            rc = mqtt.MQTT_ERR_SUCCESS

            while rc == mqtt.MQTT_ERR_SUCCESS:
                rc = self.client.loop_misc()
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    break

            if rc == mqtt.MQTT_ERR_NO_CONN:
                logger.debug('--- Reconnecting')
                try:
                    self.client.reconnect()
                except (socket.error, OSError, WebsocketConnectionError):
                    logger.debug('Reconnection failed. Retrying')
                    await asyncio.sleep(3)

