import  json
from store.cassandra.connection import Adapter
class SqlFunctions():
    __session = None
    def __init__(self):
        self.__session = Adapter().getSession()

    def insertTweet(self, tweet):
        tweetJson = json.loads(tweet) #create json instance
        id_ = tweetJson['id_str'] #get string id
        tweet = str(tweet).replace("'", "''") #escape single quetes 
        tweet = tweet.replace('"', '""') #escape double quetes
        sql = 'INSERT INTO Tweet_bank (id, data) values(\'{}\', \'{}\')'.format(id_ , tweet) #prepare sql
        self.__session.execute(sql) #execute sql