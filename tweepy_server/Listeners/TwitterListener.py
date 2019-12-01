from tweepy.streaming import StreamListener
import json
import socket
#from utils.textCleaner import textCleaner
from store.cassandra.connection import Adapter
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
   
    def __init__(self, c_socket):
        self.c_socket = c_socket
        self.Adapter = Adapter()

    def on_data(self,data):
        msg = json.loads(data)
        tweet = msg["text"]
        #tweet = textCleaner().cleanEmoji(text=tweet)
        msg["text"] = tweet
        self.Adapter.insertTweet(data)
        self.c_socket.send(bytes(str(msg),'utf-8'))
        return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))