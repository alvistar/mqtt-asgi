from mqtt_asgi.mcute import Mcute
from mqtt_asgi.client import MqttClient, ApplicationException
import logging

app = Mcute()


@app.action(topic='test')
async def echo(payload: bytes, instance: Mcute):
    logging.info('Echoing')
    await instance.publish('echo', payload)


@app.action(topic='exception')
async def bad(payload: bytes, instance: Mcute):
    raise Exception('bad!')

# app.register(topic='configure/+', callback=my_action)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    server = MqttClient(app)

    server.run()


