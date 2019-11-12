import inspect
import re
from typing import List, Tuple, Callable
import logging

logger = logging.getLogger(__name__)

class Dispatcher:
    callbacks: List[Tuple[str, Callable]] = []

    def __init__(self, obj: object= None):
        self.callbacks = []

        if obj is None:
            return

        for key, value in obj.__class__.__dict__.items():
            if inspect.isfunction(value):
                if hasattr(value, "dispatcher"):
                    self.callbacks.append((value.dispatcher, value.__get__(obj)))

    async def dispatch(self, topic: str, **kwargs):
        for callback in self.callbacks:
            logger.debug("Trying %s", callback[0])
            result = re.match(callback[0], topic)
            if result:

                groups = result.groups()
                args = list(groups[:-1])
                #To match #
                args = args + groups[-1].split("/")
                logger.debug("Matched {}".format(args))

                await callback[1](*args, **kwargs)


    @staticmethod
    def mqtt_to_regex(topic: str):
        # escaped = re.escape(topic)

        return topic.replace("+", "([^/]+)").replace("#", "(.*)")+"$"

    @staticmethod
    def method_topic(topic):
        def decorator(f: Callable):
            f.dispatcher = Dispatcher.mqtt_to_regex(topic)
            return f

        return decorator

    def topic(self, topic):
        def decorator(f: Callable):
            self.callbacks.append((Dispatcher.mqtt_to_regex(topic), f))
            return f

        return decorator
