from cassandra.cluster import Cluster
import json
class Adapter():
    __cluster = Cluster(['127.0.0.1'])
    __session = None
    def __init__(self):
      self.__session =   self.__cluster.connect()
      self.__session.execute('USE Tweets')
    def insertTweet(self, tweet):
        tweetJson = json.loads(tweet)
        id_ = tweetJson['id_str']
        tweet = str(tweet).replace("'", "''")
        tweet = tweet.replace('"', '""')
        sql = 'INSERT INTO Tweet_bank (id, data) values(\'{}\', \'{}\')'.format(id_ , tweet)
        print(sql)
        self.__session.execute(sql)
