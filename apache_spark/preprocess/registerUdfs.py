from pyspark.sql.functions import udf
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
from apache_spark.store.cassandra.sqlFunctions import SqlFunctions as sql
class registerUdfs():
    def registerSql(self,spark):
        save = udf(lambda x, y: sql().savePredicts(x, y))
        spark.udf.register("save", save)
        return spark