import time

import pytest
import zmq

from improv.messaging import HarvesterInfoReplyMsg


def test_harvester_shuts_down_on_sigint(setup_store, harvester):
    harvester_ports, p = harvester
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
