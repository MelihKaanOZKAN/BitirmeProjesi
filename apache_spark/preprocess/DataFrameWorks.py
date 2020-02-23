from pyspark import RDD
from pyspark.sql import  DataFrame, column
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, FloatType

class DataFrameWorks():
    def _init__(self):
        pass

    def __convertStrringType(self, str) -> StringType:
        return StringType()



    def convertDataFrame(self, rdd: RDD, SqlObject) -> DataFrame:
        """RDD to DataFrame"""
        #rdd = rdd.map(lambda l: l.replace("½",""))

        rdd =  rdd.map(lambda l: (l[:18], l[19:]))
        print(rdd.take(20))
        schema = [StructField("id", StringType(), False),
                  StructField("rawData", StringType(), False),
                  StructField("preprocessedData", ArrayType(elementType=StringType(), containsNull=True), True),
                  StructField("features", ArrayType(elementType=FloatType(), containsNull=True), True),
                  StructField("sentiment", FloatType(), True)]
        final_struct = StructType(fields=schema)
        rdd =rdd.map(lambda l: (l[0],l[1], [None],[None], None))
        return SqlObject.createDataFrame(rdd, schema=final_struct)

