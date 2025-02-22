from tweepy_server.Listeners.TwitterListener import TwitterListener
from tweepy_server.TwitterAuthenticator.TwitterAuthenticator import TwitterAuthenticator
from utils.InstructManager import InstructManager
from tweepy import Stream
from urllib3.exceptions import ProtocolError

from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
import os, http, tweepy, socket
class TwitterStreamer():
    """
    Class for streaming and processing live tweets
    """
    def __init__(self, c_socket):
        self.c_socket : socket.socket = c_socket
        self.auth = TwitterAuthenticator()
        self.listener = TwitterListener(c_socket)
        self.stream = Stream(self.auth.getAuth(), self.listener) 
        self.ıntMan = InstructManager()
    def filter(self, paramList):
        while True:
            try:
                self.stream.filter(track=paramList, stall_warnings=True)
                print("Stop Signal")
                break
            except tweepy.TweepError as e:
                print("Tweepy Error is: ", e.reason)
            except http.client.IncompleteRead as e:
                print("http exception is: ", e)
                pass
            except UnicodeEncodeError as e:
                print("Unicode error is: ", e)
                pass
            except urllib3_incompleteRead as e:
                print("IncompleteRead error is: ", e)
                continue
            # except urllib3.exceptions.ProtocolError as e:
            except ProtocolError as e:
                print("url exception is:", e)
                pass
            except UnicodeDecodeError as e:
                pass
    def sendTweets(self,tweets):
        from utils.hdfsClient import client
        cl = client()
        count = 0
        for i in tweets:
            id = i["tweetid"]
            text = i["tweet_text"]
            msg2 = id + text
            if cl.read("/tweepy/" + self.sentimentId + "_stop.txt") == "stop":
                print("Stop Signal")
                break
                self.c_socket.send("<tweet>{}</tweet>\n".format(msg2).encode("utf-8"))
                print(count)
                count = count + 1
    def checkIns(self):
        try:
            argv = []
            f = self.ıntMan.readInstruct()
            argv = f.split("\n")
            #argv = argv.split(' ')
            print(argv)
            if(argv[0] ==   "--ID"):
                sentimentId = argv[1]
                self.listener.sentimentId  = sentimentId
                pid = int(os.getpid())
                from tweepy_server.store.cassandra.sqlFunctions import SqlFunctions
                sq = SqlFunctions()
                #sq.insertPid(pid, sentimentId)
                paramList = argv[2:]
                if(len(paramList) == 0):
                    print("Error argv len")
                else:
                    #self.c_socket.send(bytes("Stream  tweets for this parameter(s):", 'utf-8'))
                    #for i in paramList:
                        #self.c_socket.send(bytes(i,'utf-8'))
                    if(paramList[0] == "--filter"):
                        paramList= paramList[1:]
                        self.filter(paramList=paramList),
                    if (paramList[0] == "--resume"):
                        from tweepy_server.store.cassandra.sqlFunctions import SqlFunctions
                        sql = SqlFunctions()
                        tweets = sql.getTweets(sentimentId)
                        sendTweets(tweets)



            elif (argv[0] == "--help"):
                here = os.path.dirname(os.path.abspath(__file__))
                filename = os.path.join(here, 'help.txt')
                with open(filename,  "r") as f:
                    self.c_socket.send(bytes(f.read(), 'utf-8'))
            else:
                self.c_socket.send(bytes("Wrong arguments.. Use --help for help", 'utf-8'))
        except Exception as e:
            print("Error.." + str(e))
            self.c_socket.close()

