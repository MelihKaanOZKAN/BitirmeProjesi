from utils.CassandraConnector import Adapter
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import threading
class SqlFunctions(threading.Thread):
    def __init__(self):
        self.session = Adapter().getSession()

    def savePredicts(self, id, predict, text):
        sql = "UPDATE db.tweet_bank SET pdt_sentiment=?, tweet_text_cleaned=? where tweetid=?"
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        sql = self.session.prepare(sql)
        batch.add(sql,  (predict, text, id))
        """tmp = "["
        for index, i in enumerate(text):
            a = "'{}'".format(i)
            if index < len(text) -1:
                tmp += "{},".format(a)
            else:
                tmp += "{}".format(a)
        tmp += "]"
        sql = "UPDATE db.tweet_bank SET pdt_sentiment={}, tweet_text_cleaned={} where tweetid='{}'".format(str(predict), tmp, id)"""
        self.session.execute(batch)
        return 1


