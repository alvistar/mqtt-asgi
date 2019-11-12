from typing import Callable
from dataclasses import dataclass
import logging
import re
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class Action:
    topic: str
    callback: Callable
    subscribe: bool = True


def dispatch(topic: str, registry: List[Action]):
    for action in registry:
        logger.debug("Trying %s", action.topic)
        result = re.match(mqtt_to_regex(action.topic), topic)

        if result:
            groups = result.groups()

            if len(groups) > 0:
                args = list(groups[:-1])
                # Last group can be # and contains multiple args
                args = args + groups[-1].split("/")
                logger.debug("Matched {}".format(args))
            else:
                args = []

            return action, args

        return None, []


def mqtt_to_regex(topic: str):
    # escaped = re.escape(topic)

    return topic.replace("+", "([^/]+)").replace("#", "(.*)")+"$"

