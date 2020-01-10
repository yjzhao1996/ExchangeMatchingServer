#!/usr/bin/python3
"""
March 26, 2019
Runs as a server connected on port 12345
"""

_author_= "Prathikshaa Rangarajan"
_maintainer_ = "Prathikshaa Rangarajan"

# ref: https://stackoverflow.com/questions/1603109/how-to-make-a-python-script-run-like-a-service-or-daemon-in-linux
# ref: https://docs.python.org/3.3/howto/sockets.html
# ref: https://www.binarytides.com/receive-full-data-with-the-recv-socket-function-in-python/

import socket # for sockets
import sys # for exit
import time

class user_socket:

    def __init__(self, sock=None):
        if sock is None:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            pass
        else:
            self.sock = sock
            pass
        pass

    def bind(self, port, host=None):
        if host is None:
            host = socket.gethostname()
            pass
        self.sock.bind((host, port))

    def connect(self, host, port):
        self.sock.connect((host, port))
        pass

    def sendall(self, msg):
        total_sent = 0
        while total_sent < len(msg):
            sent = self.sock.send(msg[total_sent:])
            if sent == 0:
                raise RuntimeError("send: socket connection closed")
            total_sent = total_sent + sent
            pass
        pass

    def recvall(self, total_msg_len):
        msg = b''
        while(len(msg)) < total_msg_len:
            part = self.sock.recv(total_msg_len-len(msg))
            if part == b'':
                raise RuntimeError("recv: socket connection closed")
            msg = msg + part
            pass
        return msg
    pass

# todo: move below code to main
# Create a TCP socket
server_socket = user_socket()
server_socket.bind(12345)
