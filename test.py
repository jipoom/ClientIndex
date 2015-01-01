import socket
import sys

HOST, PORT = "localhost", 9999
data = "Hi How r u?"

# Create a socket (SOCK_STREAM means a TCP socket)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Connect to server and send data
    sock.connect((HOST, PORT))
    while 1:
        sock.sendall(data + "\n")
        
        # Receive data from the server and shut down
        received = sock.recv(1024)
        print "Sent:     {}".format(data)
        print "Received: {}".format(received)
finally:
    sock.close()

