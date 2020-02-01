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

    def clean(self, dataFrame: DataFrame, sqlContext : SQLContext, ):
        textCleaner = udf(lambda x: tc.preprocess(tweet=x), StringType())
        self.__spark.udf.register("textCleaner", textCleaner)
        df2 : DataFrame = dataFrame.select(textCleaner(dataFrame("rawData")))
        return df2
