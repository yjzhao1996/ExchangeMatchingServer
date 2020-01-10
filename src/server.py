#!/usr/bin/python3
#socket_echo_server.py                                                                                           
import socket
import sys
import threading

from xml_parser_header import parse_xml
from database_connect import *
from database_setup import *
from response import *


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
pass


def recv_until_nl(sock):
    num=""
    temp=sock.recv(1)
    temp = temp.decode('utf-8')
    while temp!='\n':
        num+=temp
        temp=sock.recv(1)
        temp = temp.decode('utf-8')
    return num



def process_request(connection, client_address):
    try:
        print('connection from', client_address)
        # Receive the data in small chunks and retransmit it
        #length=recvall(connection,28)
        num=recv_until_nl(connection)
        len=int(num)
        #recv_string=recvall(connection,int.from_bytes(length, byteorder='big'))
        recv_string=recvall(connection,len)
        obj=parse_xml(recv_string)
        # for sub in obj.sequence:
        #     if sub.type=='account':
        #         obj = create_account(connect(),sub)
        #         print(obj)
        #         print("Error:")
        #         print(obj.err)
        #     if sub.type=='position':
        #         obj = create_position(connect(),sub)
        #         print(obj)
        #         print("Error:")
        #         print(obj.err)
        response = []
        for sub in obj.sequence:
            if sub.type == 'account':
                db_conn = connect()
                new_obj = create_account(db_conn, sub)
                response.append(new_obj)
                print(new_obj)
                print("Error:")
                print(new_obj.err)
                db_conn.close()
            if sub.type == 'position':
                db_conn = connect()
                new_obj = create_position(db_conn, sub)
                response.append(new_obj)
                print(new_obj)
                print("Error:")
                print(new_obj.err)
                db_conn.close()
            if sub.type == 'order':
                db_conn = connect()
                new_obj = create_order(db_conn, sub, obj.account_id)
                response.append(new_obj)
                print(new_obj)
                print("Error:")
                print(new_obj.err)
                db_conn.close()
            if sub.type == 'query':
                db_conn = connect()
                new_obj = query_order(db_conn, sub)
                response.append(new_obj)
                print(new_obj)
                print("Error:")
                print(new_obj.err)
                db_conn.close()
            if sub.type =='cancel':
                db_conn = connect()
                new_obj = cancel_order(db_conn,sub)
                response.append(new_obj)
                print(new_obj)
                print("Error:")
                print(new_obj.err)
                db_conn.close()

        if obj.type == 'create':
            connection.send(create_response(response).encode('utf-8'))
        if obj.type == 'transac':
            connection.send(transaction_response(response).encode('utf-8'))


    finally:
        # Clean up the connection
        connection.close()



# Create a TCP/IP socket                                                                                         
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# Bind the socket to the port                                                                                    
server_address = (socket.gethostname(), 12345)
print('starting up on {} port {}'.format(*server_address))
sock.bind(server_address)

# Listen for incoming connections                                                                                
sock.listen(1)

while True:
    # Wait for a connection                                                                                      
    print('waiting for a connection')
    connection, client_address = sock.accept()
    t=threading.Thread(target=process_request(connection,client_address))
    t.start()


