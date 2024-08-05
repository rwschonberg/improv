import zmq
import logging

from zmq.log.handlers import PUBHandler


def main():
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUB)
    s.connect("tcp://localhost:7005")
    handler = PUBHandler(interface_or_socket=s, root_topic="logging_topic")
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    logger.info("test")


main()
