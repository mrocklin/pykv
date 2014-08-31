import socket
import zmq
from toolz import curry, merge, partial
from toolz.curried import valmap, pipe, merge_with
from toolz.compatibility import range
import dill
import json
from sys import stderr

context = zmq.Context()

serialize = dill.dumps
deserialize = dill.loads
serialize = json.dumps
deserialize = json.loads

union = lambda L: set.union(*L)

log = open('log', 'w')

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
        request = deserialize(self.server.recv())
        log.write('%s: %s\n' % (self.url, request))
        log.flush()
        if isinstance(request, (tuple, list)):
            func = getattr(self, request[0].replace('-', '_'))
            response = func(*request[1:])
        elif isinstance(request, dict):
            op = request.pop('op')
            func = getattr(self, op.replace('-', '_'))
            response = func(**request)
        self.server.send(serialize(response))

    def event_loop(self):
        """ Keep handling messages on our server/input socket """
        while True:
            self.handle()

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
        log.write('%s: created sockets' % self.url)
        log.flush()

        # The information we know about that we want to share with everyone
        neighbors = merge({self.url: self.data.keys()},
                          self.neighbors)
        neighbors = valmap(list, neighbors)

        # Send that information
        for sock in socks:
            sock.send(serialize(('share_neighbors', neighbors)))
        log.write('%s: sent share_neighbors messages\n' % self.url)
        log.flush()

        # Listen for return information and merge it with our own
        for sock in socks:
            log.write('%s: waiting on %s\n' % (self.url, sock))
            log.flush()
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
