from tweepy_server.Streamers.TwitterStreamer import TwitterStreamer
import socket
import threading
class ClientThread(threading.Thread):
    def __init__(self,clientAddress,clientsocket):
        threading.Thread.__init__(self)
        self.csocket = clientsocket
        self.object = None
        print ("New connection added: ", clientAddress)
    def run(self):
        print ("Connection from : ", clientAddress)
        #self.csocket.send(bytes("Hi, This is from Server, waitin for instructions",'utf-8'))
       # msg = input()
        while True:
            #data = self.csocket.recv(2048)
           # msg = data.decode()
            #msg = self.argv
            if(self.object is None):
                self.object = TwitterStreamer(self.csocket)
                self.object.checkIns()
                  
                
            #self.csocket.send(bytes(msg,'UTF-8'))
        print ("Client at ", clientAddress , " disconnected...")
LOCALHOST = "192.168.1.62"
PORT = 1998
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind((LOCALHOST, PORT))
print("Server started")
print("Waiting for client request..")
while True:
    server.listen(5)
    clientsock, clientAddress = server.accept()
    newthread = ClientThread(clientAddress, clientsock)
    newthread.start()


