import sys,os
os.environ.setdefault("JAVA_HOME","/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home")


from pyspark import RDD
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext

from preprocess.DataFrameWorks import DataFrameWorks as DF
from preprocess.CleanText  import CleanText as CD

class sparkManager():

    def __init__(self, hostname: str, port: int, appname_: str, master: str):
        self.__hostname = hostname
        self.__port = port
        self.__appName = appname_
        print("Initializing Spark Instance")
        self.__sc = SparkContext(appName=self.__appName, master=master)
        self.__ssc = StreamingContext(self.__sc, 2)
        self.__spark = SQLContext(self.__sc)
        self.__dataStream = self.__ssc.socketTextStream(hostname=self.__hostname, port=self.__port)
        self.__sc.setLogLevel("ERROR")
        print("Spark Inıtıalized")

    def startStreaming(self):
        rdds = self.__dataStream.window(20)
        rdds.foreachRDD(lambda rdd:  self.__preprocessRdd(rdd))
        self.__ssc.start()



    def __preprocessRdd(self, rdd:RDD):
        if(rdd.isEmpty() == False):
           df = DF().convertDataFrame(rdd, self.__spark)
           CD().clean(df, self.__spark).show()


sm = sparkManager(hostname="localhost", port=1998, appname_="test", master='local[*]')
sm.startStreaming()