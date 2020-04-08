from utils.CassandraConnector import Adapter
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import threading
class SqlFunctions(threading.Thread):
    def __init__(self):
        self.session = Adapter().getSession()

    def savePredicts(self, id, predict, text):
        sql = "UPDATE sentiment_analysis.tweet_bank SET pdt_sentiment=?, tweet_text_cleaned=? where tweetid=?"
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        sql = self.session.prepare(sql)
        batch.add(sql, (predict, text, id))
        self.session.execute(batch)
        return 1
