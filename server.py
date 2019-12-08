import socket,sys,datetime, ast
import _thread as thread
from chord import *

users_database = []
users = {}
pending_messages = {}
addr = Address(sys.argv[1],sys.argv[2])
mutex = thread.allocate_lock()

join_addr = None
if len(sys.argv) >= 4:
    join_addr = Address(sys.argv[3],int(sys.argv[4]))
node = LocalNode(addr,join_addr,'server')


ls = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
ls.bind((sys.argv[1],int(sys.argv[2])))
ls.listen(10)

def _recv(sock:socket.socket):
    data=bytes()
    while True:
        da = sock.recv(1024)
        if not da: break
        data += da
    return data

def loggin(sock:socket.socket):
    b = False
    sock.send('Type the User name'.encode())
    username = sock.recv(1024).decode()
    sock.send('Type the password'.encode())
    p = sock.recv(1024).decode() 
    
    for (u,p) in users_database:
        if u == user and password == p:
            users[u] = sock
            b = True
    if len(pending_messages[u]) > 0:
        for message in pending_messages[u]:
            sock.send(message.encode())
    sock.send('ok'.encode())
    mutex.acquire()
    users[username] = sock
    mutex.release()
    return b

def sign_up(sock:socket.socket):
    sock.send('Type a username:'.encode())
    username = sock.recv(1024).decode()
    for (u,_) in users_database :
        if u == username:
            sock.send('The user name has alredy in use, choose another'.encode())
            return False
    sock.send('Type a password:'.encode())
    p = sock.recv(1024)
    users_database.append((username,p))
    sock.send('ok'.encode())
    mutex.acquire()
    users[username] = sock
    mutex.release()
    return True
        
def connection(sock:socket.socket):
    sock.send('Whatsapp server\nType the number of the option you want\n1- Sign In\n2- Sign Up'.encode())
    try:
        while True:
            data = sock.recv(1024).decode()
            head = data.split(':')[0]
            harr = head.split('/')
            print(data)
            if data == '1':
                while not loggin(sock):
                    sock.send('The username or password are incorrect or does not exist. Try again'.encode())
                continue
            elif data == '2':
                while not sign_up(sock):continue
                continue
            elif data == 'update':
                update_me(sock)
                continue

            if harr[len(harr)-1] == 'public':
                text = data.split(':')[1]
                for key in users.keys():
                        if key == harr[0]: continue
                        users[key].send((harr[0]+':'+text).encode())
            else:
                people = harr[1:len(harr)-1]
                text = harr[0] + ':'+ data.split(':')[1]
                for p in people:
                    if p in users.keys():
                        users[p].send(text.encode())
                    else:
                        snode = node.looking_for_server(node.id())
                        if snode != None and snode.id() != node.id():
                            ss = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                            ss.connect((node.address(0),node.address(1)))
                        else:
                            pending_messages[p].append((datetime.datetime.now(),text))
                            update_other()

    except:
        thread.exit()
   

def update_other():
    save()
    (snode,idlist) = node.looking_for_server(node.id())
    if snode != None and snode.id() != node.id():
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect(snode.address)
        sock.send('update'.encode())
        sock.send(str(pending_messages).encode())
        sock.send(str(users).encode())
        sock.send(str(idlist))

def save():
    open('pending.txt','w').write(str(pending_messages))
    open('database.txt','w').write(str(users))
    

def update_me(sock:socket.socket):
    data = sock.recv(1024).decode()
    pending = sock.recv(1024).decode()
    idlist = ast.literal_eval(sock.recv(1024).decode())
    pending = ast.literal_eval(pending)
    
    for p in pending.keys():
        if p in pending_messages.keys():
            index = 0
            rang = len(pending[p])
            for message in pending[p]:
                while rang > 1:
                    index = rang/2
                    if pending_messages[p][index] > pending[p]:
                        index+= index/2
                    elif pending_messages[p][index] < pending[p]:
                        index-= index/2
                    else:break
                pending_messages[p].insert(index,message)
        else:
            pending_messages[p] = pending[p]
    
node.join(join_addr)
print('Server online!')
while True:
    sc , d = ls.accept()
    thread.start_new_thread(connection,(sc,))
   

     