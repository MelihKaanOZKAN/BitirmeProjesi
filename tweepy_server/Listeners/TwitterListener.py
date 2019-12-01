from tweepy.streaming import StreamListener
import json
import socket
#from utils.textCleaner import textCleaner
from store.cassandra.sqlFunctions import SqlFunctions
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
   
    def __init__(self, c_socket):
        self.c_socket = c_socket
        self.sqlFunctions = SqlFunctions()

    def on_data(self,data):
        msg = json.loads(data)
        tweet = msg["text"]
        #tweet = textCleaner().cleanEmoji(text=tweet)
        msg["text"] = tweet
        self.sqlFunctions.insertTweet(data)
        self.c_socket.send(bytes(str(msg),'utf-8'))
        return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))