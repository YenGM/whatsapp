
# reads from socket until "\r\n"
def read_from_socket(s):
	result = bytes()
	while 1:
		data = s.recv(256)
		if not data or data == '':
			break
		result += data
#	if result != "":
#		print "read : %s" % result
	return result

# sends all on socket, adding "\r\n"
def send_to_socket(s, msg):
#	print "respond : %s" % msg
	s.sendall((str(msg)).encode())
