from tweepy.streaming import StreamListener
import json
import socket, sys, os, time
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
        self.sqlFunctions = SqlFunctions()
        self.jsonF = JsonFormatter()

    def on_data(self,data):
        try:
            msg = json.loads(data)


            msg =   msg["id_str"] + msg["text"]
            self.sqlFunctions.insertTweet(data)
            #msg["text"].replace('"','\\"')
            #msg = "{\"id\":\""+  msg["id_str"] + "\", \"raw_data\":\"" +  msg["text"] + "\"}"
            #msg = self.jsonF.format(msg)


            # msg = msg.replace('="', "=>'")
            self.c_socket.send("<tweet>{}</tweet>\n".format(msg).encode("utf-8"))
            return True
        except KeyError:
            return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))