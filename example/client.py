from mqtt_asgi.mcute import Mcute, Session
from mqtt_asgi.server import MqttServer
import logging

app = Mcute()


@app.action(topic='configure/#')
async def my_action(*args, session: Session, payload: bytes):
    print(f'with args {args} and payload: {payload}')
    await session.publish('hello', b'world')
    await session.publish('ciao', b'mondo')

# app.register(topic='configure/+', callback=my_action)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    server = MqttServer(app)
    server.run()
