import sys,os
os.environ.setdefault("JAVA_HOME","/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home")


from pyspark import RDD
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming import StreamingContext
from preprocess.DataFrameWorks import DataFrameWorks as DF
from preprocess.CleanText  import CleanText as CD
class sparkManager():

    __SAEngine = None
    def __init__(self, hostname: str, port: int, appname_: str, master: str):
        self.__hostname = hostname
        self.__port = port
        self.__appName = appname_
        print("Initializing Spark Instance")
        conf = SparkConf()
        conf.setAppName(self.__appName)
        conf.setMaster(master)
        self.__sc = SparkContext(conf=conf)
        self.__sc.addPyFile('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/utils/textCleaner.py')
        self.__ssc = StreamingContext(self.__sc, 2)
        self.__spark = SQLContext(self.__sc)
        self.__dataStream = self.__ssc.socketTextStream(hostname=self.__hostname, port=self.__port)
        self.__sc.setLogLevel("ERROR")
        print("Spark Inıtıalized")

    def startStreaming(self):
        rdds = self.__dataStream.window(20)
        rdds.foreachRDD(lambda rdd:  self.__preprocessRdd(rdd))
        self.__ssc.start()
        self.__ssc.awaitTermination()
        rdds = self.__dataStream.window(20)


    def __preprocessRdd(self, rdd:RDD):
        if(rdd.isEmpty() == False):
           df = DF().convertDataFrame(rdd, self.__spark)
           CD().clean(df, self.__spark).show()

sm = sparkManager(hostname="192.168.1.62", port=1998, appname_="test", master='spark://100.86.196.11:7077')
sm.startStreaming()