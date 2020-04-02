from tweepy.streaming import StreamListener
import json
import sys, os

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.JsonFormatter import JsonFormatter
#from utils.textCleaner import textCleaner
from tweepy_server.store.cassandra import SqlFunctions
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
    sentimentId = 0
    def __init__(self, c_socket):
        self.c_socket = c_socket
        self.sqlFunctions = SqlFunctions()
        self.jsonF = JsonFormatter()

    def on_data(self,data):
        try:
            msg = json.loads(data)
            self.sqlFunctions.insertTweet( self.sentimentId, data)
            msg = msg["id_str"] + msg["text"]
            self.c_socket.send("<tweet>{}</tweet>\n".format(msg).encode("utf-8"))
            return True
        except Exception:
            return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))