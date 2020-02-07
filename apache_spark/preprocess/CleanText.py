from pyspark.sql import DataFrame, column
from pyspark.sql.types import StringType, ArrayType
import sys
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi')
from utils.textCleaner import textCleaner as tc


class CleanText():
    def __init__(self):
        pass

    def clean(self, dataFrame: DataFrame, spark:SQLContext ):
        tc_ = tc()
        textCleaner = udf(lambda x: tc_.preprocess(tweet=x), StringType())
        spark.udf.register("textCleaner", textCleaner)
        dataFrame = dataFrame.filter('rawData != ""')
        dataFrame = dataFrame.filter('rawData != " "')
        return dataFrame.withColumn("preprocessedData", textCleaner(dataFrame['rawData']))

