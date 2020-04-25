import sys,os,threading
from signal import signal, SIGUSR1
os.environ.setdefault("JAVA_HOME","/Library/Java/JavaVirtualMachines/jdk1.8.0_241.jdk/Contents/Home")
from pyspark import RDD
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
sys.path.append("..")
from apache_spark.preprocess.DataFrameWorks import DataFrameWorks
from apache_spark.preprocess.CleanText import CleanText
from apache_spark.SAEngine.naiveBayesModel import naiveBayes
from utils.logger import logger
from utils.rddCorrector import rddCorrector
from apache_spark.store.cassandra.save import save
class sparkManager(threading.Thread):
    __SAEngine = None
    def __init__(self, hostname: str, port: int, sentimentId: str, master: str):
        self.logger = logger()
        self.logger.setSentiemtnId(sentimentId)
        self.__hostname = hostname
        self.__port = port
        self.__appName = "sentiment_" + sentimentId
        self.logger.log("info", "Initializing Spark Instance")
        conf = SparkConf()
        conf.setAppName(self.__appName)
        conf.setMaster(master)
        conf.set("spark.executor.memory","4G")
        conf.set("spark.driver.memory","4G")
        conf.set("spark.network.timeout", "600s")
        """conf.set("spark.cassandra.connection.host","134.122.166.110")
        conf.set("spark.cassandra.connection.port","9042")"""
        self.__sc: SparkContext = SparkContext.getOrCreate(conf)
        self.__sc.setLogLevel("ERROR")
        self.__ssc = StreamingContext(self.__sc, batchDuration=10)
        self.__spark = SQLContext(self.__sc)
        self.__dataStream = self.__ssc.socketTextStream(hostname=self.__hostname, port=self.__port)
        self.logger.log("info","Spark Inıtıalized")

    def quiet(self):
        log4j = self.__sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    def startStreaming(self):
        self.logger.log("info","Starting stream.. ");
        rdds = self.__dataStream.window(20,20)
        rdds.foreachRDD(lambda rdd:  self.analyze(rdd))
        self.__ssc.start()
        self.__ssc.awaitTermination()

    def stopStreaming(self):
        self.logger.log("info","Stopping stream")
        tmp = self.isContextRunning()
        import time
        while tmp:
            time.sleep(2)
            tmp = self.isContextRunning()
        self.stopSparkContext()



    def stopSparkContext(self):
        self.logger.log("info","Stopping SparkContext")
        self.__sc.stop()



    def setNaiveBayes(self):
        self.__SAEngine = naiveBayes.naiveBayes(log= self.logger, customSparkContext=self.__sc)

    count = 0
    def isContextRunning(self):
        jobs = self.__sc.statusTracker().getActiveStageIds()
        if len(jobs) == 1 and jobs[0] == 5:
            if self.count > 3:
                return False

            self.count = self.count + 1
            return True
        else:
            return True


    def analyze(self, rdd):
        df = self.__preprocessRdd(rdd)
        if df is not None:
            df = self.__SAEngine.predict_df(df)
            self.save_(df).show(20, False)


    def save_(self, df):
        save_ = save()
        return save_.savePredicts_DF(df, self.__spark)

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
        return None
def smhandler(sm, sentimentId):
    def handler(signal_received, frame):
        if (sm != None):
            import sys
            sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
            from utils.hdfsClient import client
            tmp = client()
            tmp.overwrite(path="/tweepy/" + sentimentId + "_stop.txt", data="stop")
            sm.stopStreaming()
    return handler

if __name__ == "__main__":
    args = sys.argv[1:]
    host = ""
    port = 0
    sentimentId = ""
    master = ""
    method = ""
    #sparkManager(hostname="192.168.1.62", port=1998, sentimentId="1245", master="spark://192.168.1.33:7077")
    try:
        for index, i in enumerate(args):
            if i == "--host":
                host = args[index + 1]
            if i == "--port":
                port = int(args[index + 1])
            if i == "--sentimentId":
                sentimentId = args[index + 1]
            if i == "--master":
                master = args[index + 1]
            if i == "--method":
                method = args[index + 1]
        if host == "" or port == 0 or sentimentId == "" or master == "" or method == "":
            raise KeyError
        sm = sparkManager(hostname=host,port=port, sentimentId=sentimentId, master=master)
        if(method == "naiveBayes"):
            sm.setNaiveBayes()
        else:
            raise Exception("Error: " + method + " doesn't exist")


        signal(SIGUSR1, smhandler(sm, sentimentId))
        sm.startStreaming()
    except KeyError:
        print("Error. Wrong arguments")