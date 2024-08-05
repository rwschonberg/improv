import asyncio
import json
import time

import pytest
import zmq
from improv.actor import Actor
from zmq import SocketOption

from improv.link import ZmqLink


@pytest.fixture
def test_sub_link():
    """Fixture to provide a commonly used Link object."""
    ctx = zmq.Context()
    link_socket = ctx.socket(zmq.SUB)
    link_socket.bind("tcp://*:0")
    link_socket_string = link_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
    link_socket_port = int(link_socket_string.split(":")[-1])

    link_pub_socket = ctx.socket(zmq.PUB)
    link_pub_socket.connect(f"tcp://localhost:{link_socket_port}")

    link_socket.poll(timeout=0)  # open libzmq bug (not pyzmq)

    link_socket.subscribe("test_topic")
    time.sleep(2)

    link_socket.poll(timeout=0)

    link = ZmqLink(link_socket, "test_link", "test_topic")
    yield link, link_pub_socket
    link.socket.close(linger=0)
    link_pub_socket.close(linger=0)


@pytest.fixture
def test_pub_link():
    """Fixture to provide a commonly used Link object."""
    ctx = zmq.Context()
    link_socket = ctx.socket(zmq.PUB)
    link_socket.bind("tcp://*:0")
    link_socket_string = link_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
    link_socket_port = int(link_socket_string.split(":")[-1])

    link_sub_socket = ctx.socket(zmq.SUB)
    link_sub_socket.connect(f"tcp://localhost:{link_socket_port}")
    link_sub_socket.subscribe("test_topic")

    link = ZmqLink(link_socket, "test_link", "test_topic")
    yield link, link_sub_socket
    link.socket.close(linger=0)
    link_sub_socket.close(linger=0)


def test_put(test_pub_link):
    """Tests if messages can be put into the link.

    TODO:
        Parametrize multiple test input types.
    """

    link, link_sub_socket = test_pub_link
    msg = "message"
    time.sleep(1)
    # this is bad, but it doesn't work without this.
    # I wonder if something in the zmq async execution pool just needs
    # a moment to get set up, and going straight through to the test
    # catches it in a bad state

    link.put(msg)
    res = link_sub_socket.recv_multipart()
    assert res[0].decode("utf-8") == "test_topic"
    assert json.loads(res[1].decode("utf-8")) == msg


def test_put_unserializable(test_pub_link):
    """Tests if an unserializable object raises an error.

    Instantiates an actor, which is unserializable, and passes it into
    Link.put().

    Raises:
        SerializationCallbackError: Actor objects are unserializable.
    """
    time.sleep(1)
    act = Actor("test", "/tmp/store")
    link, link_sub_socket = test_pub_link
    sentinel = True
    try:
        link.put(act)
    except Exception:
        sentinel = False

    assert not sentinel


def test_put_irreducible(test_pub_link, setup_store):
    """Tests if an irreducible object raises an error."""

    link, link_sub_socket = test_pub_link
    store = setup_store
    with pytest.raises(TypeError):
        link.put(store)


def test_put_nowait(test_pub_link):
    """Tests if messages can be put into the link without blocking.

    TODO:
        Parametrize multiple test input types.
    """

    time.sleep(1)

    link, link_sub_socket = test_pub_link
    msg = "message"

    t_0 = time.perf_counter()

    link.put(msg)  # put is already async even in synchronous zmq

    t_1 = time.perf_counter()
    t_net = t_1 - t_0
    assert t_net < 0.005  # 5 ms


def test_put_multiple(test_pub_link):
    """Tests if async putting multiple objects preserves their order."""

    link, link_sub_socket = test_pub_link

    time.sleep(1)

    messages = [str(i) for i in range(10)]

    messages_out = []

    for msg in messages:
        link.put(msg)

    for msg in messages:
        messages_out.append(
            json.loads(link_sub_socket.recv_multipart()[1].decode("utf-8"))
        )

    assert messages_out == messages


@pytest.mark.asyncio
async def test_put_and_get_async(test_pub_link):
    """Tests if async get preserves order after async put."""

    messages = [str(i) for i in range(10)]

    messages_out = []

    time.sleep(1)

    link, link_sub_socket = test_pub_link

    for msg in messages:
        await link.put_async(msg)

    for msg in messages:
        messages_out.append(
            json.loads(link_sub_socket.recv_multipart()[1].decode("utf-8"))
        )

    assert messages_out == messages


@pytest.mark.parametrize(
    "message",
    [
        "message",
        "",
        None,
        [str(i) for i in range(5)],
    ],
)
def test_get(test_sub_link, message):
    """Tests if get gets the correct element from the queue."""

    link, link_pub_socket = test_sub_link

    if type(message) is list:
        for i in message:
            link_pub_socket.send_multipart(
                [link.topic.encode("utf-8"), json.dumps(i).encode("utf-8")]
            )
        expected = message[0]
    else:
        link_pub_socket.send_multipart(
            [link.topic.encode("utf-8"), json.dumps(message).encode("utf-8")]
        )
        expected = message

    assert link.get() == expected


def test_get_empty(test_sub_link):
    """Tests if get blocks if the queue is empty."""

    link, unused = test_sub_link

    with pytest.raises(TimeoutError):
        link.get(timeout=0.5)


@pytest.mark.parametrize(
    "message",
    [
        "message",
        "",
        ([str(i) for i in range(5)]),
    ],
)
def test_get_nowait(test_sub_link, message):
    """Tests if get_nowait gets the correct element from the queue."""

    link, link_pub_socket = test_sub_link

    if type(message) is list:
        for i in message:
            link_pub_socket.send_multipart(
                [link.topic.encode("utf-8"), json.dumps(i).encode("utf-8")]
            )
        expected = message[0]
    else:
        link_pub_socket.send_multipart(
            [link.topic.encode("utf-8"), json.dumps(message).encode("utf-8")]
        )
        expected = message

    time.sleep(1)

    t_0 = time.perf_counter()

    res = link.get_nowait()

    t_1 = time.perf_counter()

    assert res == expected
    assert t_1 - t_0 < 0.005  # 5 msg


def test_get_nowait_empty(test_sub_link):
    """Tests if get_nowait raises an error when the queue is empty."""

    link, unused = test_sub_link
    with pytest.raises(TimeoutError):
        link.get_nowait()


@pytest.mark.asyncio
async def test_get_async_success(test_sub_link):
    """Tests if async_get gets the correct element from the queue."""

    link, link_pub_socket = test_sub_link
    msg = "message"
    link_pub_socket.send_multipart(
        [link.topic.encode("utf-8"), json.dumps(msg).encode("utf-8")]
    )
    res = await link.get_async()
    assert res == "message"


@pytest.mark.skip(reason="Stuck in lock contention loop.")
@pytest.mark.asyncio
async def test_get_async_empty(test_sub_link):
    """
    Tests if get_async times out given an empty queue.
    """

    link, scratch = test_sub_link
    timeout = 5.0

    with pytest.raises(asyncio.TimeoutError):
        task = asyncio.create_task(link.get_async())
        await asyncio.wait_for(task, timeout)
        task.cancel()

    link.real_executor.shutdown(wait=False, cancel_futures=True)
