from __future__ import annotations

import signal

import zmq

from improv.link import ZmqLink
from improv.store import RedisStoreInterface
from zmq import SocketOption

from improv.messaging import HarvesterInfoMsg


def bootstrap_harvester(
        nexus_hostname,
        nexus_port,
        redis_hostname,
        redis_port,
        broker_hostname,
        broker_port, filename):
    harvester = RedisHarvester(nexus_hostname, nexus_port, redis_hostname, redis_port, broker_hostname, broker_port)
    harvester.register_with_nexus()
    harvester.serve(harvester.collect, filename)


class RedisHarvester:
    def __init__(self, nexus_hostname, nexus_comm_port, redis_hostname, redis_port, broker_hostname, broker_port):
        self.link: ZmqLink | None = None
        self.running = True
        self.nexus_hostname: str = nexus_hostname
        self.nexus_comm_port: int = nexus_comm_port
        self.redis_hostname: str = redis_hostname
        self.redis_port: int = redis_port
        self.broker_hostname: str = broker_hostname
        self.broker_port: int = broker_port
        self.zmq_context: zmq.Context | None = None
        self.nexus_socket: zmq.Socket | None = None
        self.sub_port: int | None = None
        self.sub_socket: zmq.Socket | None = None
        self.store_client: RedisStoreInterface | None = None

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            signal.signal(s, self.shutdown)

    def register_with_nexus(self):
        # connect to nexus
        self.zmq_context = zmq.Context()
        self.zmq_context.setsockopt(SocketOption.LINGER, 0)
        self.nexus_socket = self.zmq_context.socket(zmq.REQ)
        self.nexus_socket.connect(f"tcp://{self.nexus_hostname}:{self.nexus_comm_port}")

        self.sub_socket = self.zmq_context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{self.broker_hostname}:{self.broker_port}")
        sub_port_string = self.sub_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
        self.sub_port = int(sub_port_string.split(":")[-1])
        self.sub_socket.subscribe("")  # receive all incoming messages

        self.store_client = RedisStoreInterface("harvester", self.redis_port, self.redis_hostname)

        self.link = ZmqLink(self.sub_socket, "harvester", "")

        port_info = HarvesterInfoMsg(
            "harvester",
            "Ports up and running, ready to serve messages",
        )

        self.nexus_socket.send_pyobj(port_info)
        self.nexus_socket.recv_pyobj()

        return

    def serve(self, message_process_func, filename):
        with open(filename, "a+") as file:
            while self.running:
                # this is more testable but may have a performance overhead
                message_process_func(file)

    def collect(self, file):
        # calculate the size of the store currently
        # if the size is over 75% capacity
        #   while the size is over 50% capacity
        #     pull a message off the link
        #     fetch the data from the store without decompression (raw access to client)
        #     save the data to disk
        #     perform flush if we need to
        #     expire the key in Redis
        pass

    def shutdown(self, signum, frame):
        print("shutting down due to signal {}".format(signum))
        if self.sub_socket:
            self.sub_socket.close(linger=0)

        if self.nexus_socket:
            self.sub_socket.close(linger=0)

        if self.zmq_context:
            self.zmq_context.destroy(linger=0)

        self.running = False
