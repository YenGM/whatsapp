import socket,sys
import _thread as thread
from chord import *

addr = Address(sys.argv[1], sys.argv[2])
join_addr = None
if len(sys.argv) >= 4:
    join_addr = Address(sys.argv[3],int(sys.argv[4]))
node = LocalNode(addr,join_addr,'client')

mutex = thread.allocate_lock()

rs = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
addr = (sys.argv[1],int(sys.argv[2]))
username = ''
rs.connect(addr)

def _recv(sock:socket.socket):
    data=bytes()
    while True:
        da = sock.recv(1024)
        print(da.decode())
        if not da: break
        data += da
    return data

def recieve():
    try:
        while True:
            data = rs.recv(1024)
            print(data.decode())
    except:
        snode = node.looking_for_server(node.id())
        if snode != None:
            rs.connect(snode.address)
        else: 
            print('No servers availables.')
            thread.exit()
       


def init():
    global username
    print(rs.recv(1024).decode())
    rs.send(input().encode())
    print(rs.recv(1024).decode()) 
    username = input()
    rs.send(username.encode())
    print(rs.recv(1024).decode())
    rs.send(input().encode())
    data = rs.recv(1024).decode()
    if data == 'ok': return
    else: init()
    
def start():
    thread.start_new_thread(recieve,())
    while True: 
        head = username + '/'
        text = head + input()
        if text == 'exit': 
            rs.close()
            break
        mutex.acquire()
        rs.send(text.encode())
        mutex.release()

init()
start()