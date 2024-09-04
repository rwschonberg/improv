import base64
import pickle
import time
import zlib

import pytest
import zmq
from improv.store import RedisStoreInterface

from improv.link import ZmqLink
from improv.messaging import HarvesterInfoReplyMsg


def test_harvester_shuts_down_on_sigint(setup_store, harvester):
    harvester_ports, broker_socket, p = harvester
    ctx = zmq.Context()
    s = ctx.socket(zmq.REP)
    s.bind(f"tcp://*:{harvester_ports[3]}")
    s.recv_pyobj()
    reply = HarvesterInfoReplyMsg("harvester", "OK", "")
    s.send_pyobj(reply)
    time.sleep(2)
    p.terminate()
    p.join(5)
    if p.exitcode is None:
        p.kill()
        pytest.fail("Harvester did not exit in time")
    else:
        assert True


def test_harvester_dumps_to_file(setup_store, harvester):
    harvester_ports, broker_socket, p = harvester
    broker_link = ZmqLink(broker_socket, "test", "test topic")
    ctx = zmq.Context()
    s = ctx.socket(zmq.REP)
    s.bind(f"tcp://*:{harvester_ports[3]}")
    s.recv_pyobj()
    reply = HarvesterInfoReplyMsg("harvester", "OK", "")
    s.send_pyobj(reply)
    client = RedisStoreInterface()
    for i in range(9):
        message = [i for i in range(500000)]
        key = client.put(message)
        broker_link.put(key)
    time.sleep(5)
    p.terminate()
    p.join(5)
    if p.exitcode is None:
        p.kill()
        pytest.fail("Harvester did not exit in time")

    with open("test_harvester_out.bin", "r") as f:
        data = f.readlines()
        for line in data:
            record = pickle.loads(zlib.decompress(base64.b64decode(line)))
            assert len(record) == 500000
