#!/usr/bin/python3
#socket_echo_client.py
import socket
import sys

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
server_address = ('vcm-8946.vm.duke.edu', 10000)
print('connecting to {} port {}'.format(*server_address))
sock.connect(server_address)

try:

    # Send data
    #message = b'This is the message.  It will be repeated.'
    #print('sending {!r}'.format(message))
    filename = 'test.xml'
    f = open(filename, 'rb')
    l = f.read(1024)
    while(l):
        sock.send(l)
        l = f.read(1024)
    f.close()
        #sock.sendall(message)

    # Look for the response
    """
    amount_received = 0
    amount_expected = len(message)

    while amount_received < amount_expected:
        data = sock.recv(16)
        amount_received += len(data)
        print('received {!r}'.format(data))
    """

finally:
    print('closing socket')
    sock.close()
