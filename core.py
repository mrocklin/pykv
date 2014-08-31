import socket
import zmq
from toolz import curry, merge, partial, first, second
from toolz.curried import valmap, pipe, merge_with, groupby, map
from toolz.compatibility import range
import dill
import json
import threading
from sys import stderr

context = zmq.Context()

serialize = dill.dumps
deserialize = dill.loads
serialize = json.dumps
deserialize = json.loads

union = lambda L: set.union(*L)

class Node(object):
    """

    hostname: string
        The name of this machine
    port: int
        The port on which I listen
    data: dict
        The data that we serve
    neighbors: dict
        mapping of {url: {set-of-keys}} known to exist on each neighbor
    server: zmq.Socket
        The server on which we listen for requests
    """
    def __init__(self, port, hostname=None, data=None):
        self.hostname = hostname or socket.gethostname()
        self.data = data or dict()
        self.port = port
        self.neighbors = dict()
        self.server = context.socket(zmq.REP)
        self.server.bind("tcp://*:%s" % self.port)

    def handle(self):
        """ Receive message, dispatch internally, send response """
        if self.server.closed:
            raise Exception()
        request = deserialize(self.server.recv())
        if request == 'close':
            self._stop = True
            self.server.send(serialize('Closing'))
            return
        if isinstance(request, (tuple, list)):
            func = getattr(self, request[0].replace('-', '_'))
            response = func(*request[1:])
        elif isinstance(request, dict):
            op = request.pop('op')
            func = getattr(self, op.replace('-', '_'))
            response = func(**request)
        self.server.send(serialize(response))

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value
        return 'OK'

    @property
    def url(self):
        return 'tcp://%s:%d' % (self.hostname, self.port)


    def update(self):
        """ Share neighbor information with our neighbors

        This node broadcasts its knowledge about its neighborhood to its
        neighborhood.  It expects to hear back about it neighbors'
        neighborhoods
        """
        # Create a socket out to each known neighbor
        socks = []
        for neighbor in self.neighbors:
            sock = context.socket(zmq.REQ)
            sock.connect(neighbor)
            socks.append(sock)

        # The information we know about that we want to share with everyone
        neighbors = merge({self.url: self.data.keys()},
                          self.neighbors)
        neighbors = valmap(list, neighbors)

        # Send that information
        for sock in socks:
            sock.send(serialize(('share_neighbors', neighbors)))

        # Listen for return information and merge it with our own
        for sock in socks:
            self.neighbors = pipe(sock.recv(),
                                  deserialize,
                                  valmap(set),
                                  partial(merge_with, union, self.neighbors))

    def share_neighbors(self, neighbors):
        """ Update another node's information about myself

        Someone else gave us a dict of neighbor information.  Lets record it
        and then send back the information that we know.
        """
        del neighbors[self.url]
        neighbors = valmap(set, neighbors)
        self.neighbors = merge_with(union, neighbors, self.neighbors)

        return merge(valmap(list, self.neighbors),
                     {self.url: list(self.data.keys())})

    def event_loop(self):
        """ Keep handling messages on our server/input socket """
        self._stop = False
        while not self._stop:
            self.handle()

    def start(self):
        self.thread = threading.Thread(target=self.event_loop)
        self.thread.start()

    def stop(self):
        sock = context.socket(zmq.REQ)
        sock.connect(self.url)
        sock.send(serialize('close'))
        self._stop = True

    def catalog(self):
        acquisitions = ((url, key) for url, keys in self.neighbors.items()
                                   for key in keys)

        return pipe(acquisitions, groupby(second),
                                  valmap(map(first)),
                                  valmap(set))

class BigDict(object):
    """ Dict interface to distributed kv-store

    Parameters
    ----------

    node: Node
        An entry point to the kv network
    """
    def __init__(self, node):
        self.node = node

    def get(self, key):
        if key in self.node.data:
            return self.node.data[key]
        else:
            cat = self.node.catalog()
            sock = context.socket(zmq.REQ)
            url = first(cat[key])
            sock.connect(url)
            request = serialize(('get', key))
            sock.send(request)
            response = deserialize(sock.recv())
            return response

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.node.data[key] = value
