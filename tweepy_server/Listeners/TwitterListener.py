from tweepy.streaming import StreamListener
import json
import sys, os, time, socket

sys.path.insert(1, os.path.join(sys.path[0], '..'))
from utils.JsonFormatter import JsonFormatter
#from utils.textCleaner import textCleaner
from utils.hdfsClient import client
from tweepy_server.store.cassandra import SqlFunctions
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
class TwitterListener(StreamListener):
    """
    Basic twitter listener
    """
    sentimentId = 0
    count = 0
    def __init__(self, c_socket):
        self.c_socket: socket.socket = c_socket
        self.sqlFunctions = SqlFunctions()
        self.jsonF = JsonFormatter()
        self.cl = client()

    def on_exception(self, exception):
        print(exception)
        return
    def on_data(self,data):
        try:
            if self.cl.read("/tweepy/" + self.sentimentId + "_stop.txt") == "stop":
                return False
            msg = json.loads(data)
            if msg["lang"] == "en":
                self.sqlFunctions.insertTweet( self.sentimentId, data)
                msg2 = msg["id_str"] + msg["text"]
                self.c_socket.send("<tweet>{}</tweet>\n".format(msg2).encode("utf-8"))
                self.count = self.count +1
                print(self.count)
            return True
        except BrokenPipeError:
            print("Broken Pipe")
            return False
        except BaseException as e:
            print("Error on_data: %s, Pausing..." % str(e))
            return True
        except http_incompleteRead as e:
            print("http.client Incomplete Read error: %s" % str(e))
            # restart stream - simple as return true just like previous exception?
            return True
        except urllib3_incompleteRead as e:
            print("urllib3 Incomplete Read error: %s" % str(e))
            return True

    def on_error(self, status):
        return False