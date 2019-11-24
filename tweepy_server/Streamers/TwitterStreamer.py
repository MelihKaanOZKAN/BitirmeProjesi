from Listeners.TwitterListener import TwitterListener  
from TwitterAuthenticator.TwitterAuthenticator import TwitterAuthenticator
from tweepy import Stream
import threading, os
class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """
    def __init__(self, c_socket):
        self.c_socket = c_socket
        self.auth = TwitterAuthenticator()
        self.listener = TwitterListener(c_socket)
        self.stream = Stream(self.auth.getAuth(), self.listener) 

    def checkIns(self):
        argv = []
        here = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(here, 'instructions.txt')
        with open(filename,  "r") as f:
            for tmp in f:
                argv.append(tmp.replace('\n',''))
        #argv = argv.split(' ')
        print(argv)
        if(argv[0] ==   "--filter"):
            paramList = argv[1:]
            if(len(paramList) == 0):
                print("a")
                #self.c_socket.send(bytes"/msg/"+":/"Error: Missing parameters."/", 'utf-8'))
            else:
                #self.c_socket.send(bytes("Stream  tweets for this parameter(s):", 'utf-8'))
                #for i in paramList:
                    #self.c_socket.send(bytes(i,'utf-8'))
                
                self.stream.filter(track=paramList, is_async=True)
        elif (argv[0] == "--help"):
            here = os.path.dirname(os.path.abspath(__file__))
            filename = os.path.join(here, 'help.txt')
            with open(filename,  "r") as f:
                self.c_socket.send(bytes(f.read(), 'utf-8'))
        else:
            self.c_socket.send,(bytes("Wrong arguments.. Use --help for help", 'utf-8'))
        
