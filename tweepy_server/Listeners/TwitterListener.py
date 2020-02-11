from tweepy.streaming import StreamListener
import json
import socket, sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.JsonFormatter import JsonFormatter
#from utils.textCleaner import textCleaner
from store.cassandra.sqlFunctions import SqlFunctions
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
   
    def __init__(self, c_socket):
        self.c_socket = c_socket
        #self.sqlFunctions = SqlFunctions()
        self.jsonF = JsonFormatter()

    def on_data(self,data):
        try:
            msg = json.loads(data)
          #  self.sqlFunctions.insertTweet(data)
            print("------")
            print(msg)
            print("------")
            msg = msg["text"]
            msg = self.jsonF.format(msg)
            #msg = msg.replace('="', "=>'")
            self.c_socket.send(msg.encode("utf-8"))
            return True
        except KeyError:
            return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))