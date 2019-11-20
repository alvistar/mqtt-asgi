import pytest

from mqtt_asgi.client import MqttClient


@pytest.mark.timeout(3)
@pytest.mark.asyncio
async def test_on_disconnect_connected():
    client = MqttClient(application=None)
    client.connected.set()

    client.on_disconnect()

    assert not client.mqtt_q.empty()

    msg = await client.mqtt_q.get()

    assert msg == {'type': 'mqtt_disconnect'}


@pytest.mark.asyncio
async def test_on_disconnect_disconnected():
    client = MqttClient(application=None)

    client.on_disconnect()

    assert client.mqtt_q.empty()
