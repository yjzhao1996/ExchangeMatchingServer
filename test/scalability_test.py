import socket
import sys
import os

import random, string, time
from xml.etree.ElementTree import Element, SubElement
from ElementTree_pretty import prettify

def recvall(sock,total_msg_len):
    msg = b''
    while(len(msg)) < total_msg_len:
        part = sock.recv(total_msg_len-len(msg))
        if part == b'':
            #raise RuntimeError("recv: socket connection closed")
            break
        msg = msg + part
        pass
    return msg

def randomword(length):
   letters = string.ascii_uppercase
   return ''.join(random.choice(letters) for i in range(length))

def create_request():
    top=Element('create')
    attributes1={"id":str(random.randint(1,10001)),"balance":str(random.randint(1,10001))}
    SubElement(top, 'account', attributes1)
    attributes2={"sym":randomword(3)}
    node=SubElement(top,'symbol',attributes2)
    attributes3={"id":str(random.randint(1,10001))}
    node1=SubElement(node,'account',attributes3)
    node1.text=str(random.randint(1,10001))
    return prettify(top)

def transaction_request():
    top=Element('transactions')
    attributes1={'id':str(random.randint(1,10001))}
    top.attrib=attributes1
    attributes2={'sym':randomword(3),'amount':str(random.randint(-10000,10001)),'limit':str(random.randint(1,10001))}
    SubElement(top,'order',attributes2)
    attributes3={'id':str(random.randint(1,10001))}
    SubElement(top,'query',attributes3)
    attributes4={'id':str(random.randint(1,10001))}
    SubElement(top,'cancel',attributes4)
    return prettify(top)

# Create a TCP/IP socket
numbers=int(sys.argv[1])
result=[0,0,0,0,0,0]

p_start=int(round(time.time() * 1000))

while(numbers):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect the socket to the port where the server is listening
    server_address = ('vcm-9355.vm.duke.edu', 12345)
    #print('connecting to {} port {}'.format(*server_address))
    sock.connect(server_address)



    start=int(round(time.time() * 1000))
    sent=create_request()
    length=len(sent)
    sock.send(length.to_bytes(28,'big'))
    sock.send(sent.encode())
    # length = recvall(sock, 28)
    # recv_string = recvall(sock, int.from_bytes(length, byteorder='big'))
    #print(sock.recv(2048))
    sock.recv(2048)
    end=int(round(time.time() * 1000))
    #print(str(end-start))
    gap=end-start
    if(gap<=10):
        result[0]+=1
    elif(gap>10 and gap<=20):
        result[1]+=1
    elif (gap > 20 and gap <= 30):
        result[2] += 1
    elif (gap > 30 and gap <= 40):
        result[3] += 1
    elif (gap > 40 and gap <= 50):
        result[4] += 1
    elif (gap > 50 ):
        result[5] += 1
    numbers-=1

p_end=int(round(time.time() * 1000))

for i in result:
    print(i)

print("Total time: "+str(p_end-p_start)+" ms")
