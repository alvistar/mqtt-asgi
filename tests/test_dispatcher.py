import pytest
from dispatcher import Action, dispatch


def test_check_single_argument():
    action = Action(topic='my_topic/+', callback=None)

    matched, args = dispatch('my_topic/123', registry=[action])

    assert matched == action

    assert len(args) == 1

    assert args[0] == '123'


def test_check_multiple_arguments():
    action = Action(topic='my_topic/#', callback=None)

    matched, args = dispatch('my_topic/123/325', registry=[action])

    assert matched == action

    assert args == ['123', '325']


def test_no_match():
    action = Action(topic='my_topic/#', callback=None)

    matched, args = dispatch('my_topics/323', registry=[action])

    assert matched is None

    assert len(args) == 0


def test_no_args():
    action = Action(topic='my_topic', callback=None)

    matched, args = dispatch('my_topic', registry=[action])

    assert matched == action

    assert len(args) == 0
