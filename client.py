#!/usr/bin/env python

import os

import dill as pickle
import zmq


class Client(object):
    """An API for calling functions in a remote context.
    
    Like RPC, but where the P is defined locally instead of pre-registered remotely.
    """
    
    def __init__(self, url, context=None):
        self.url = url
        self._context = context or zmq.Context.instance()
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.connect(url)
    
    def apply(self, f, *args, **kwargs):
        """Submit a function to be called on a server and return the result"""
        msg = [
            b'apply_request',
            pickle.dumps(dict(
                f=f, args=args, kwargs=kwargs,
            )),
        ]
        self._socket.send_multipart(msg)
        reply = self._socket.recv_multipart()
        assert reply[0] == b'apply_reply'
        return pickle.loads(reply[1])

if __name__ == '__main__':
    c = Client('tcp://127.0.0.1:5555')
    print("Client PID = %s" % os.getpid())
    print("Server PID = %s" % c.apply(os.getpid))
    print("    5 * 10 = %s" % c.apply(lambda x, y: x * y, 5, y=10))
    print("     1 / 0 = %r" % c.apply(lambda x, y: x / y, 1, 0))
