from core import *
import multiprocessing
import threading
from contextlib import contextmanager
import time

port = 5455
context = zmq.Context()


@contextmanager
def node_at(port):
    n = Node(port)
    n.start()
    try:
        yield n
    finally:
        n.server.close()
        n.stop()


def test_get_set():
    with node_at(5455) as node:
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://localhost:5455')

        socket.send_json(('set', 'key1', 'value1'))
        assert socket.recv_json() == 'OK'

        socket.send_json(('set', 'key2', 'value2'))
        assert socket.recv_json() == 'OK'

        socket.send_json(('get', 'key1'))
        assert socket.recv_json() == 'value1'


def test_update_with_neighbors():
    with node_at(5456) as A:
        with node_at(5457) as B:
            # Give each a bit of data
            A.data[1] = 'one'
            B.data[2] = 'two'

            # add B to A's neighbor list
            A.neighbors[B.url] = set()

            # Share between neighbors
            A.update()

            assert A.url in B.neighbors
            assert B.url in A.neighbors
            assert 2 in A.neighbors[B.url]
