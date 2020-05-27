import numpy as np
seed = 7
np.random.seed(seed)
from keras.models import Sequential
from keras.layers import Dense, Dropout
from keras.layers import Flatten
from keras.layers.embeddings import Embedding
from keras.preprocessing import sequence
from apache_spark.SAEngine.trainData import TrainData
from apache_spark.SAEngine.SAEngine import SAEngine
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from apache_spark.preprocess.CleanText import CleanText
class NeuralNetwork():
    __documents = []
    __documents_test = []
    __modelPath = "hdfs://78.186.219.59:9000/NeuralNetwork.model"
    __trainPath = "hdfs://40.87.68.229:9000/trainingdata.csv"
    __testPath = "hdfs://192.168.1.33:9000/testdata.csv"
    def __init__(self, log, customSparkContext=None):
        self.logger = log
        self.logger.log("info", "Initializing Naive Bayes..")
        self.logger.log("info", "Initializing Spark..")
        if customSparkContext == None:
            conf = SparkConf()
            conf.setAppName("model-NeuralNetwork")
            conf.setMaster("spark://192.168.1.33:7077")
            conf.set("spark.sql.warehouse.dir", "file:///C:\spark_warehouse")
            conf.set("spark.executor.memory", "3G")
            conf.set("spark.driver.memory", "3G")
            self.__sc = SparkContext(conf=conf)
            self.__sc.setLogLevel("ERROR")
            fs = (self.__sc._jvm.org
                  .apache.hadoop
                  .fs.FileSystem
                  .get(self.__sc._jsc.hadoopConfiguration())
                  )

        else:
            self.logger.log("info", "Using custom SparkContext.. Skipping.. ")
            self.__sc = customSparkContext
            self.loadModelFromDisk()
        self.__SQLContext = SQLContext(self.__sc)
        self.logger.log("info", "Complate..")

    def load_train(self):
        train_df = self.__SQLContext.read.csv(header=True, path=self.__trainPath)
        train_df = train_df.select("text", "sentiment").withColumnRenamed("text", "rawData").withColumnRenamed(
            "sentiment", "label")
        train_df = train_df.withColumn("label", train_df["label"].cast(IntegerType()))
        train_df = self.cleanTextOnDF(train_df)
        train_df = self.preproccess(train_df)
        return train_df

    def load_test(self):
        test_df = self.__SQLContext.read.csv(header=True, path=self.__testPath)
        test_df = test_df.select("text", "sentiment").withColumnRenamed("text", "rawData").withColumnRenamed(
            "sentiment", "label")
        test_df = test_df.withColumn("label", test_df["label"].cast(IntegerType()))
        test_df = self.cleanTextOnDF(test_df)
        test_df = self.preproccess(test_df)
        return test_df

    def cleanTextOnDF(self, df: DataFrame):
        ct = CleanText()
        spark = self.__SQLContext
        df = ct.clean(dataFrame=df, spark=spark)
        return df

    def __preprocess_tdfidf(self, df: DataFrame):
        hashingTF = HashingTF().setInputCol("preprocessedData").setOutputCol("tf").setNumFeatures(1500000)
        idf = IDF().setInputCol("tf").setOutputCol("features")
        df = hashingTF.transform(df)
        df_model = idf.fit(df)
        df = df_model.transform(df)
        return df

    def preproccess(self, df: DataFrame):
        df = self.__preprocess_tdfidf(df)
        return df


    def createModel(self):
        train = self.load_train()
        test = self.load_test()

