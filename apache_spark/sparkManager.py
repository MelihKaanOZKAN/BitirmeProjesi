import sys,os
os.environ.setdefault("JAVA_HOME","/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home")

from pyspark import RDD
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
sys.path.append("..")
from apache_spark.preprocess.DataFrameWorks import DataFrameWorks
from apache_spark.preprocess.CleanText import CleanText
from apache_spark.SAEngine.naiveBayesModel import naiveBayes
from utils.rddCorrector import rddCorrector
from apache_spark.store.cassandra.save import save
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
        conf.set("spark.executor.memory","4G")
        conf.set("spark.driver.memory","4G")
        self.__sc = SparkContext(conf=conf)
        self.__ssc = StreamingContext(self.__sc, batchDuration=5)
        self.__spark = SQLContext(self.__sc)
        self.__dataStream = self.__ssc.socketTextStream(hostname=self.__hostname, port=self.__port)
        self.__sc.setLogLevel("ERROR")
        print("Spark Inıtıalized")


    def startStreaming(self):
        rdds = self.__dataStream
        rdds.foreachRDD(lambda rdd:  self.analyze(rdd))
        self.__ssc.start()
        self.__ssc.awaitTermination()

    def setNaiveBayes(self):
        self.__SAEngine = naiveBayes.naiveBayes(customSparkContext=self.__sc)


    def analyze(self, rdd):
        df = self.__preprocessRdd(rdd)
        df.show()
        if df is not None:
            df = self.__SAEngine.predict_df(df)
            save(df)

    def save(self, df):
        save_ = save()
        save_.save(df, self.__spark)

    def __preprocessRdd(self, rdd:RDD):
        rddc = rddCorrector()
        rdd = rdd.map(lambda l: rddc.correct(l))
        if rdd != None:
            if(rdd.isEmpty() == False):
                rdd = rdd.map(lambda l: l.replace("<tweet>",""))
                rdd = rdd.map(lambda l: l.replace("</tweet>",""))
                df = DataFrameWorks().convertDataFrame(rdd, self.__spark)
                df = CleanText().clean(df, self.__spark)
                return df





sm = sparkManager(hostname="192.168.1.33", port=1998, appname_="test2", master='spark://78.186.219.59:7077')
#sm = sparkManager(hostname="192.168.1.62", port=1998, appname_="test2", master='local[*]')
sm.setNaiveBayes()
sm.startStreaming()