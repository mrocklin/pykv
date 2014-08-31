from core import *
import multiprocessing
import threading
from contextlib import contextmanager
import time

port = 5455
context = zmq.Context()


@contextmanager
def node_at(port, data=None):
    n = Node(port, data=data)
    n.start()
    try:
        yield n
    finally:
        n.stop()


def test_get_set():
    port = 5465
    with node_at(port) as node:
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://localhost:%d' % port)

        socket.send_json(('set', 'key1', 'value1'))
        assert socket.recv_json() == 'OK'

        socket.send_json(('set', 'key2', 'value2'))
        assert socket.recv_json() == 'OK'

        socket.send_json(('get', 'key1'))
        assert socket.recv_json() == 'value1'


def test_update_with_neighbors():
    with node_at(5456, {1: 'one'}) as A, node_at(5457, {2: 'two'}) as B:

        # add B to A's neighbor list
        A.neighbors[B.url] = set()

        # Share between neighbors
        A.update()

        assert A.url in B.neighbors
        assert B.url in A.neighbors
        assert 2 in A.neighbors[B.url]
        assert 1 in B.neighbors[A.url]

def test_big_dict():
    with node_at(5458, {1: 'one'}) as A, node_at(5459, {2: 'two'}) as B:
        A.neighbors[B.url] = {2}
        bd = BigDict(A)
        assert bd[1] == 'one'
        assert bd[2] == 'two'

