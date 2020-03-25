from utils.CassandraConnector import Adapter
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
import threading
class SqlFunctions(threading.Thread):
    def __init__(self):
        self.session = Adapter().getSession()
    def savePredicts(self, id, predict):
        sql = "UPDATE sentiment_analysis.tweet_bank SET pdt_sentiment=? where tweetid=?"
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        sql = self.__session.prepare(sql)
        batch.add(sql, (predict, id))
        self.__session.execute(batch)
        return 1
    def savePredicts_DF(self, df:DataFrame, spark : SQLContext):
        save = udf(lambda x,y: self.savePredicts(x,y))
        spark.udf.register("save", save)
        return df.withColumn("save_status", save(df["id"], df["sentiment"]))