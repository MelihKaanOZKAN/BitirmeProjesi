from apache_spark.store.cassandra.sqlFunctions import SqlFunctions as sql
from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
class save():
    def savePredicts_DF(self, df:DataFrame, spark : SQLContext):
        save = udf(lambda x, y: sql().savePredicts(x, y))
        spark.udf.register("save", save)
        return df.withColumn("save_status", save(df["id"], df["sentiment"]))
