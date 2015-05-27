#!/usr/bin/env python

import dill as pickle
import zmq


class Server(object):
    """An RPC server for arbitrary code execution"""
    
    def __init__(self, url, context=None):
        self.url = url
        self._context = context or zmq.Context.instance()
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.bind(url)
        self._poller = zmq.Poller()
        self._poller.register(self._socket, zmq.POLLIN)
    
    def run(self):
        """Inner loop, wait for messages"""
        while True:
            events = dict(self._poller.poll())
            if self._socket in events:
                self.handle_msg(self._socket.recv_multipart())
        
    def handle_msg(self, msg):
        """Handle an apply request"""
        client, msg = msg[0], msg[1:]
        assert msg[0] == b'apply_request'
        request = pickle.loads(msg[1])
        f = request['f']
        args = request['args']
        kwargs = request['kwargs']
        
        # return the result or the exception
        try:
            result = f(*args, **kwargs)
        except Exception as e:
            result = e
        
        # send the reply
        self._socket.send_multipart([
            client,
            b'apply_reply',
            pickle.dumps(result)
        ])

if __name__ == '__main__':
    s = Server('tcp://127.0.0.1:5555')
    s.run()
