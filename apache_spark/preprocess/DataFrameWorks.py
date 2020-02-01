from pyspark import RDD
from pyspark.sql import  DataFrame, column
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType

class DataFrameWorks():
    def _init__(self):
        pass

    def __convertStrringType(self, str) -> StringType:
        return StringType()
    def convertDataFrame(self, rdd: RDD, SqlObject) -> DataFrame:
        """RDD to DataFrame"""
        schema = [StructField("rawData", StringType(), False),
                  StructField("preprocessedData", ArrayType(elementType=StringType(), containsNull=True), True),
                  StructField("sentiment", IntegerType(), True)]
        final_struct = StructType(fields=schema)
        rdd =rdd.map(lambda l: (l, [None], None))
        return SqlObject.createDataFrame(rdd, schema=final_struct)
