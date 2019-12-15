"""Script for Tkinter GUI chat client."""
from socket import AF_INET, socket, SOCK_STREAM
import  json, os, sys, csv
from datetime import datetime
from threading import Thread
dir_path = os.path.dirname(os.path.realpath(__file__))
parent_dir_path = os.path.abspath(os.path.join(dir_path, os.pardir))
sys.path.insert(0, parent_dir_path)
from utils.textCleaner import textCleaner
class CreateSample():
    now = datetime.now()
    here = os.path.dirname(os.path.abspath(__file__))
    filename_test = os.path.join(here, 'tweets_test_ ' + now.strftime("%d_%m_%Y-%H:%M:%S")+'.csv')   
    filename_train = os.path.join(here, 'tweets_train_ ' + now.strftime("%d_%m_%Y-%H:%M:%S")+'.csv')   
    def __init__(self, train_count, test_count):
        self.train = train_count
        self.test = test_count
        self.tc = textCleaner()
    
    def createTest(self, tweet):
        if(self.test != 0):
            print("------")
            print(tweet)
            print("------")
            ptext =  self.tc.preprocess(tweet["text"])
            
            with open(self.filename_test, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([tweet["id"], ptext])
            self.test = self.test - 1
        else:
            self.createTrain(tweet)
    def createTrain(self, tweet):
        if(self.train != 0):
            print("------")
            print(tweet)
            print("------")
            ptext =  self.tc.preprocess(tweet["text"])
            with open(self.filename_train, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([tweet["id"], ptext])
            self.train = self.train - 1
        else:
            sys.exit(0)
def receive():
    """Handles receiving of messages."""
    SampleMaker = CreateSample(300, 100)
    while True:
        try:
            msg = client_socket.recv(30000).decode("utf-8")
            print("e------")
            print(msg)
            print("e------")
            msg = json.loads(msg, )
            # msg = encoder.convert(msg)
            #msg_list.insert(tkinter.END, msg )
            SampleMaker.createTest(msg)
            
                
        except OSError:  # Possibly client has left the chat.
            pass






#----Now comes the sockets part----
HOST = '127.0.0.1'
PORT = 1998
if not PORT:
    PORT = 1998
else:
    PORT = int(PORT)

ADDR = (HOST, PORT)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(ADDR)

receive_thread = Thread(target=receive)
receive_thread.start()