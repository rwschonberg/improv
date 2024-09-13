import logging
import multiprocessing
import os
import signal
import time

import pytest
import subprocess

import zmq

from improv.actor import ZmqActor
from improv.harvester import bootstrap_harvester
from improv.nexus import Nexus

redis_port_num = 6379
WAIT_TIMEOUT = 10

SERVER_COUNTER = 0


@pytest.fixture
def ports():
    global SERVER_COUNTER
    CONTROL_PORT = 5555
    OUTPUT_PORT = 5556
    LOGGING_PORT = 5557
    ACTOR_IN_PORT = 7005
    yield (
        CONTROL_PORT + SERVER_COUNTER,
        OUTPUT_PORT + SERVER_COUNTER,
        LOGGING_PORT + SERVER_COUNTER,
        ACTOR_IN_PORT + SERVER_COUNTER,
    )
    SERVER_COUNTER += 4


@pytest.fixture
def setdir():
    prev = os.getcwd()
    os.chdir(os.path.dirname(__file__) + "/configs")
    yield None
    os.chdir(prev)


@pytest.fixture
def set_dir_config_parent():
    prev = os.getcwd()
    os.chdir(os.path.dirname(__file__))
    yield None
    os.chdir(prev)


@pytest.fixture
def sample_nex(setdir, ports):
    nex = Nexus("test")
    try:
        nex.create_nexus(
            file="good_config.yaml",
            store_size=40000000,
            control_port=ports[0],
            output_port=ports[1],
        )
    except Exception as e:
        print(f"error caught in test harness: {e}")
        logging.error(f"error caught in test harness: {e}")
    yield nex
    try:
        nex.destroy_nexus()
    except Exception as e:
        print(f"error caught in test harness: {e}")
        logging.error(f"error caught in test harness: {e}")


@pytest.fixture
def server_port_num():
    return redis_port_num


@pytest.fixture
# TODO: put in conftest.py
def setup_store(server_port_num):
    """Start the server"""
    p = subprocess.Popen(
        [
            "redis-server",
            "--save",
            '""',
            "--port",
            str(server_port_num),
            "--maxmemory",
            str(10000000),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    yield p

    # kill the subprocess when the caller is done with it
    p.send_signal(signal.SIGINT)
    try:
        p.wait(timeout=WAIT_TIMEOUT)
    except subprocess.TimeoutExpired:
        logging.error("Timed out waiting for redis to close.")
        try:
            p.kill()
            p.wait(timeout=WAIT_TIMEOUT)
        except subprocess.TimeoutExpired:
            logging.error("Timed out waiting for redis to close after kill.")
            raise Exception("Redis fixture didn't close")


def nex_startup(ports, filename):
    nex = Nexus("test")
    nex.create_nexus(
        file=filename,
        store_size=100000000,
        control_port=ports[0],
        output_port=ports[1],
        actor_in_port=ports[3],
    )
    nex.start_nexus()


@pytest.fixture
def start_nexus_minimal_zmq(ports):
    filename = "minimal.yaml"
    p = multiprocessing.Process(target=nex_startup, args=(ports, filename))
    p.start()
    time.sleep(1)

    yield p

    p.terminate()
    p.join(WAIT_TIMEOUT)
    if p.exitcode is None:
        logging.exception("Timed out waiting for nexus to stop")
        p.kill()


# make a fixture to spool up an actor
# do it just like the nexus test; spin off actor target which calls nexus connect method
# imitate what nexus would do (connect to ports), and send it to the actor
# assert on getting a response back from the actor's signal port


@pytest.fixture
def zmq_actor(ports):
    actor = ZmqActor(ports[3], None, None, None, None, None, name="test")

    p = multiprocessing.Process(target=actor_startup, args=(actor,))

    yield p

    p.terminate()
    p.join(WAIT_TIMEOUT)
    if p.exitcode is None:
        p.kill()


def actor_startup(actor):
    actor.register_with_nexus()


@pytest.fixture
def harvester(ports):
    ctx = zmq.Context()
    socket = ctx.socket(zmq.PUB)
    socket.bind("tcp://*:1234")
    p = multiprocessing.Process(
        target=bootstrap_harvester,
        args=(
            "localhost",
            ports[3],
            "localhost",
            6379,
            "localhost",
            1234,
            "localhost",
            12345,
        ),
    )
    p.start()
    time.sleep(1)
    yield ports, socket, p
    socket.close(linger=0)
