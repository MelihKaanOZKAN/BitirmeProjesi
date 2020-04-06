from tweepy_server.Listeners import TwitterListener
from tweepy_server.TwitterAuthenticator.TwitterAuthenticator import TwitterAuthenticator
from utils.InstructManager import InstructManager
from tweepy import Stream
from utils.hdfsClient import client
import os
class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """
    def __init__(self, c_socket):
        self.c_socket = c_socket
        self.auth = TwitterAuthenticator()
        self.listener = TwitterListener(c_socket)
        self.stream = Stream(self.auth.getAuth(), self.listener) 
        self.ıntMan = InstructManager()
    def checkIns(self):
        try:
            argv = []
            f = self.ıntMan.readInstruct()
            argv = f.split("\n")

            #argv = argv.split(' ')
            print(argv)
            if(argv[0] ==   "--ID"):
                self.listener.sentimentId  = int(argv[1])
                paramList = argv[2:]
                if(len(paramList) == 0):
                    print("a")
                    #self.c_socket.send(bytes"/msg/"+":/"Error: Missing parameters."/", 'utf-8'))
                else:
                    #self.c_socket.send(bytes("Stream  tweets for this parameter(s):", 'utf-8'))
                    #for i in paramList:
                        #self.c_socket.send(bytes(i,'utf-8'))
                    if(paramList[0] == "--filter"):
                        paramList= paramList[1:]
                        self.stream.filter(track=paramList, is_async=True, languages=["en"])
            elif (argv[0] == "--help"):
                here = os.path.dirname(os.path.abspath(__file__))
                filename = os.path.join(here, 'help.txt')
                with open(filename,  "r") as f:
                    self.c_socket.send(bytes(f.read(), 'utf-8'))
            else:
                self.c_socket.send,(bytes("Wrong arguments.. Use --help for help", 'utf-8'))
        except:
            print("Error..")
            self.c_socket.close()
