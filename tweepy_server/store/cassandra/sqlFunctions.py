import  json
from utils.CassandraConnector import Adapter
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
class SqlFunctions():
    __session = None
    def __init__(self):
        self.__session = Adapter().getSession()

    def insertTweet(self, sentimentId, tweet):
        tweetJson = json.loads(tweet) #create json instance
        id_ = tweetJson['id_str'] #get string id
        text = tweetJson['text']
        tweet = str(tweet).replace("'", "''") #escape single quetes 
        tweet = tweet.replace('"', '""') #escape double quetes
        sql = 'INSERT INTO tweet_bank (sentimentId, tweetId, tweet, tweet_text) values(?, ?, ?, ?)'
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        sql = self.__session.prepare(sql)
        batch.add(sql, (sentimentId, id_, tweet, text))
        self.__session.execute(batch) #execute sql