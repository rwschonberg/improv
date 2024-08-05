import zmq
from zmq import SocketOption

from improv.messaging import BrokerInfoMsg


def bootstrap_broker(nexus_hostname, nexus_port):
    broker = PubSubBroker(nexus_hostname, nexus_port)
    broker.register_with_nexus()
    broker.serve(broker.read_and_pub_message)


class PubSubBroker:
    def __init__(self, nexus_hostname, nexus_comm_port):
        self.nexus_hostname: str = nexus_hostname
        self.nexus_comm_port: int = nexus_comm_port
        self.zmq_context: zmq.Context
        self.nexus_socket: zmq.Socket
        self.pub_port: int
        self.sub_port: int
        self.pub_socket: zmq.Socket
        self.sub_socket: zmq.Socket

    def register_with_nexus(self):
        # connect to nexus
        self.zmq_context = zmq.Context()
        self.zmq_context.setsockopt(SocketOption.LINGER, 0)
        self.nexus_socket = self.zmq_context.socket(zmq.REQ)
        self.nexus_socket.connect(f"tcp://{self.nexus_hostname}:{self.nexus_comm_port}")

        self.sub_socket = self.zmq_context.socket(zmq.SUB)
        self.sub_socket.bind("tcp://*:0")
        sub_port_string = self.sub_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
        self.sub_port = int(sub_port_string.split(":")[-1])
        self.sub_socket.subscribe("")  # receive all incoming messages

        self.pub_socket = self.zmq_context.socket(zmq.PUB)
        self.pub_socket.bind("tcp://*:0")
        pub_port_string = self.pub_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
        self.pub_port = int(pub_port_string.split(":")[-1])

        port_info = BrokerInfoMsg(
            "broker",
            self.pub_port,
            self.sub_port,
            "Ports up and running, ready to serve messages",
        )

        self.nexus_socket.send_pyobj(port_info)
        self.nexus_socket.recv_pyobj()

        return

    def serve(self, message_process_func):
        while True:
            # this is more testable but may have a performance overhead
            message_process_func()

    def read_and_pub_message(self):  # receive and send back out
        msg = self.sub_socket.recv_multipart()
        self.pub_socket.send_multipart(msg)
