import hashlib
import socket
import time
import json
import base64
import ast
import random
import _thread as thread
from network import *

STABILIZE_INT = 1
STABILIZE_RET = 4
LOGSIZE = 8
SIZE = 1<<LOGSIZE
N_SUCCESSORS = 4

# Fix Fingers
FIX_FINGERS_INT = 4

# Update Successors
UPDATE_SUCCESSORS_INT = 1
UPDATE_SUCCESSORS_RET = 6

# Find Successors
FIND_SUCCESSOR_RET = 3
FIND_PREDECESSOR_RET = 3

def repeat_and_sleep(sleep_time):
	def decorator(func):
		def inner(self, *args, **kwargs):
			while 1:
				time.sleep(sleep_time)
				if self.shutdown_:
					return
				ret = func(self, *args, **kwargs)
				if not ret:
					return
		return inner
	return decorator

def retry_on_socket_error(retry_limit):
	def decorator(func):
		def inner(self, *args, **kwargs):
			retry_count = 0
			while retry_count < retry_limit:
				try:
					ret = func(self, *args, **kwargs)
					return ret
				except socket.error:
					# exp retry time
					time.sleep(2 ** retry_count)
					retry_count += 1
			if retry_count == retry_limit:
				print("Retry count limit reached, aborting.. (%s)" % func.__name__)
				self.shutdown_ = True
				sys.exit(-1)
		return inner
	return decorator

def requires_connection(func):
	""" initiates and cleans up connections with remote server """
	def inner(self, *args, **kwargs):
		self.mutex_.acquire()

		self.open_connection()
		ret = func(self, *args, **kwargs)
		self.close_connection()
		self.mutex_.release()

		return ret
	return inner


class Address(object):
    def __init__(self,ip,port):
        self.ip = ip
        self.port = port
    
    def __hash__(self):
        return hash(("%s:%s" % (self.ip, self.port))) % SIZE

    def __str__(self):
        return str((self.ip,self.port))

    def __eq__(self,Address):
        return self.ip == Address.ip and self.port == Address.port

class LocalNode(object):
    # def __init__(self, local_address, remote, ntype):
    #     self.address_ = local_address
	#     #self.shutdown_ = False
    #     self.m = 6
	# 	# list of successors
	#     self.successors_ = []
	# 	self.join(remote)
    #     self.type = ntype
    def __init__(self,local_address,remote,ntype):
        self.address = local_address
        self.shutdown_ = False
        self.m = 6
        self.successors_ = []
        self.ntype = ntype
        self.join(remote)
        print('Este es mi id %s' % self.id())
        thread.start_new_thread(self.stabilize,())
        thread.start_new_thread(self.fix_fingers,())
        thread.start_new_thread(self.update_successors,())


    def join(self,remote_address = None):
        self.finger_ = [None for i in range(LOGSIZE)]
        self.predecessor_ = None

        if remote_address:
            remote = Remote(remote_address)
            self.finger_[0] = remote.find_successor(self.id())
        else:
            self.finger_[0] = self

    def ping(self):
        return True
    
    @repeat_and_sleep(STABILIZE_INT)
    @retry_on_socket_error(STABILIZE_RET)
    def stabilize(self):
        suc = self.successor()
        if not suc.id() == self.finger_[0].id():
            self.finger_[0] = suc
        x = suc.predecessor()
        if x and \
		   self.inrange(x.id(), self.id(1), suc.id()) and \
		   self.id(1) != suc.id() and \
		   x.ping():
           self.finger_[0] = x
        self.successor().notify(self)
        node = self.successor()
        # if node:
        #     print('Este es mi sucesor %s' % node.address)
        # else: print('No tengo sucesor')
        return True
        
    def notify(self, remote):
        if not self.predecessor() or \
		self.inrange(remote.id(), self.predecessor().id(1), self.id()) or \
		not self.predecessor().ping():
            self.predecessor_ = remote

    def inrange(self,c, a, b):
        a = a % SIZE
        b = b % SIZE
        c = c % SIZE
        if a < b:
            return a <= c and c < b
        return a <= c or c < b
    
    @repeat_and_sleep(FIX_FINGERS_INT)
    def fix_fingers(self):
        i = random.randrange(LOGSIZE -1 ) + 1
        self.finger_[i] = self.find_successor(self.id(1<<i))
        return True
    
    @repeat_and_sleep(UPDATE_SUCCESSORS_INT)
    @retry_on_socket_error(UPDATE_SUCCESSORS_RET)
    def update_successors(self):
        suc = self.successor()
        if suc.id() != self.id():
            successors = [suc]
            suc_list = suc.get_successors()
            if suc_list and len(suc_list):
                successors += suc_list
            self.successors_ = successors
        return True
    
    def get_successors(self):
        return [(node.address.ip,node.address.port) for node in self.successors_[:N_SUCCESSORS -1]]
    
    def id(self, offset = 0):
        return (self.address.__hash__() + offset) % SIZE
    
    
    def successor(self):
        for remote in [self.finger_[0]] + self.successors_:
            if remote.ping():
                self.finger_[0] = remote
                return remote
        
        print("No successor available, aborting")
        self.shutdown_ = True
        sys.exit(-1)
    
    def predecessor(self):
        return self.predecessor_
    
    def find_successor(self, id):
        if self.predecessor() and \
		self.inrange(id, self.predecessor().id(1), self.id(1)):
            return self
        node = self.find_predecessor(id)
        return node.successor()
    
    def find_predecessor(self, id):
        node = self
        
        if node.successor().id() == node.id():
            return node
        while not self.inrange(id, node.id(1), node.successor().id(1)):
            node = node.closest_preceding_finger(id)
      
        return node
        
    
    def closest_preceding_finger(self, id):
        for remote in reversed(self.successors_ + self.finger_):
            if remote != None and self.inrange(remote.id(), self.id(1), id) and remote.ping():
                return remote
        return self

    def connection_manager(self,conn,data):
        command = data.split(' ')[0]
        request = data[len(command)+1:]
        
        result = json.dumps("")
        if command == 'get_successor':
            successor = self.successor()
            result = json.dumps((successor.address.ip, successor.address.port))
        if command == 'get_predecessor':
            if self.predecessor_ != None:
                predecessor = self.predecessor_
                result = json.dumps((predecessor.address.ip, predecessor.address.port))
        if command == 'find_successor':
            successor = self.find_successor(int(request))
            result = json.dumps((successor.address.ip, successor.address.port))
        if command == 'closest_preceding_finger':
            closest = self.closest_preceding_finger(int(request))
            result = json.dumps((closest.address.ip, closest.address.port))
        if command == 'notify':
            npredecessor = Address(request.split(' ')[0], int(request.split(' ')[1]))
            self.notify(Remote(npredecessor))
        if command == 'get_successors':
            result = json.dumps(self.get_successors())
        if command == 'get_type':
            result = self.ntype
        if command == 'looking':
            look = self.looking_for_server(int(request))
            if look:
                result = json.dumps((look.address.ip,look.address.port))
        send_to_socket(conn, result)
        conn.close()
        if command == 'shutdown':
            self.socket_.close()
            self.shutdown_ = True

    def looking_for_server(self, id,idlist = []):
        node = self
        temp = node
        count = 0
        while True:
            # if count == 10:return None
            # count +=1
            node = node.successor()
            if not node:break
            if node == None or node.id() == self.id(): 
                return None
            if node.id in idlist: continue
            typ = node.get_type()
            if typ.startswith('server'):
                return (node, idlist)
        node = temp
        while True:
            node = node.predecessor()
            if node == None or node.id() == self.id(): 
                return None
            if node.id in idlist: continue
            typ = node.get_type()
            if typ.startswith('server'):
                return (node,idlist)

class Remote(object):
    def __init__(self, remote_address):
        self.address = remote_address
        self.mutex_ = thread.allocate_lock()
    
    def open_connection(self):
        self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_.connect((self.address.ip, int(self.address.port)))
    
    def close_connection(self):
        self.socket_.close()
        self.socket_ = None
    
    def __str__(self):
        return "Remote %s" % self.address
    
    def id(self, offset = 0):
        return (self.address.__hash__() + offset) % SIZE
    
    def send(self, msg):
        self.socket_.sendall((msg).encode())
        self.last_msg_send_ = msg
    
    def recv(self):
        return read_from_socket(self.socket_)
    
    def ping(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.address.ip, int(self.address.port)))
            s.sendall('ping'.encode())
            s.close()
            return True
        except socket.error:
            return False
    
    @requires_connection
    def get_type(self):
        self.send('chord get_type')
        response = self.recv().decode()
        return response
    
    @requires_connection
    def command(self, msg):
        self.send(msg)
        response = self.recv().decode()
        return response
    
    @requires_connection
    def get_successors(self):
        self.send('chord get_successors')
        response = self.recv().decode()
        
        if response == "":
            return []
        response = json.loads(response)
        my_map = map(lambda address: Remote(Address(address[0], address[1])) ,response)
        return list(my_map)
    
    @requires_connection
    def successor(self):
        self.send('chord get_successor')
        response = json.loads(self.recv().decode())
        return Remote(Address(response[0], response[1]))
    
    @requires_connection
    def predecessor(self):
        self.send('chord get_predecessor')
        response = self.recv().decode()
        if response:
            return None
        response = json.loads(response)
        return Remote(Address(response[0], response[1]))
    
    @requires_connection
    def find_successor(self, id):
        self.send('chord find_successor %s' % id)
        response = json.loads(self.recv().decode())
        return Remote(Address(response[0], response[1]))
    
    @requires_connection
    def closest_preceding_finger(self, id):
        self.send('chord closest_preceding_finger %s' % id)
        response = json.loads(self.recv().decode())
        return Remote(Address(response[0], response[1]))
    
    @requires_connection
    def notify(self, node):
        self.send('chord notify %s %s' % (node.address.ip, node.address.port))

    @requires_connection
    def looking(self,id):
        self.send('chord looking %s' %id)
        response = json.loads(self.recv().decode())
        return RemoteNode(Address(response[0],response[1]))

    