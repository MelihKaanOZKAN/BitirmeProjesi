import  json, threading
from utils.CassandraConnector import Adapter
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
class SqlFunctions(threading.Thread):
    __session = None
    def __init__(self):
        self.__session = Adapter().getSession()

    def insertTweet(self, sentimentId, tweet):
        tweetJson = json.loads(tweet) #create json instance
        id_ = tweetJson['id_str'] #get string id
        text = tweetJson['text']
        tweet = str(tweet).replace("'", "''") #escape single quetes 
        tweet = tweet.replace('"', '""') #escape double quetes
        sql = 'INSERT INTO db.tweet_bank (sentimentId, tweetId, tweet, tweet_text) values(?, ?, ?, ?)'
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        sql = self.__session.prepare(sql)
        batch.add(sql, (sentimentId, id_, tweet, text))
        self.__session.execute(batch)
    def insertPid(self, pid, sentimentId):
       sql = "UPDATE db.sentiments SET pids = pids + [?] where sentimentid=?"
       batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
       sql = self.__session.prepare(sql)
       import uuid
       batch.add(sql, (pid, uuid.UUID(sentimentId)))
       self.__session.execute(batch)