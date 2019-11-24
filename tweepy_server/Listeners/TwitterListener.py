from tweepy.streaming import StreamListener
import json
import socket
from textCleaner import textCleaner
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
   
    def __init__(self, c_socket):
        self.c_socket = c_socket

    def on_data(self,data):
        msg = json.loads(data)
        tweet = msg["text"]
        tweet = textCleaner().preprocess(tweet=tweet)
        self.c_socket.send(bytes(str(tweet),'utf-8'))
        return True

    def error(self, status):
        self.c_socket.send(bytes("Error: " + status,'utf-8'))