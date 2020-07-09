import threading
import socketserver
import socket


class MockMemSession(socketserver.BaseRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        self.server.append(data)
        self.server.server.basic_handler(self.server.data, self.request)


class MockMemcachedServerInternal(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def __init__(self, address, port, request_class, server):
        super(MockMemcachedServerInternal, self).__init__(server_address=(address, port),
                                                          RequestHandlerClass=request_class)
        self.data = []
        self.server = server

    def append(self, data):
        self.data.append(data)

    def reset(self):
        self.data = []

    def log(self, msg):
        self.server.log(msg)

    def pop(self, n):
        data = b''
        if n >= len(self.data):
            data = self.data[:]
            self.data = []
        else:
            data = self.data[:n]
            self.data = self.data[n:]
        return data


class MockMemcachedServer:
    def __init__(self, address='127.0.0.1', port=52135, debug=False, handler=None):
        socketserver.TCPServer.allow_reuse_address = True
        self.debug = debug
        self.address = address
        self.port = port
        self.server = MockMemcachedServerInternal(address, port, MockMemSession, self)
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.test_handler = handler
        self.running = False

    def set_debug(self, debug):
        self.debug = debug

    def set_handler(self, handler):
        self.test_handler = handler

    def start(self):
        self.running = True
        self.log('Starting server thread at {}:{}'.format(self.address, self.port))
        self.server_thread.start()

    def basic_handler(self, data, req):
        self.log('Data: {}'.format(data))
        if self.test_handler:
            self.test_handler(data, req, self.debug)

    def stop(self):
        self.log('Shut down server')
        if self.running:
            self.server.shutdown()
        self.log('Socket close')
        self.server.socket.close()
        self.log('Joining')
        if self.running:
            self.server_thread.join()

        self.running = False
        self.log('Thread finished')

    def reset(self):
        self.test_handler = None
        self.server.reset()

    def get_host_address(self):
        return self.address, self.port

    def log(self, msg):
        if self.debug:
            print(msg)


def fake_conn():
    for info in socket.getaddrinfo('127.0.0.1', 5235, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM):
        _family, socktype, proto, _, sockaddr = info
        try:
            sock = socket.socket(_family, socktype, proto)
            sock.settimeout(10)
            s = sock
            s.connect_ex(sockaddr)
            return s
        except socket.error:
            # If we get here socket objects will be close()d via
            # garbage collection.
            pass
    else:
        # Didn't break from the loop, re-raise the last error
        raise sock_error
