import inspect
import os
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from apache_spark.SAEngine.trainData import TrainData
from apache_spark.SAEngine.SAEngine import SAEngine
from apache_spark.logger import logger
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.classification import NaiveBayesModel
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,IntegerType
import utils.naiveBayesPredict as nbp
from apache_spark.preprocess.CleanText import CleanText
class naiveBayes(SAEngine):
    __trainData  = TrainData()
    __documents = []
    __documents_test = []
    __modelPath = "hdfs://78.186.219.59:9000/NaiveBayes.model"
    __trainPath = "hdfs://40.87.68.229:9000/trainingdata.csv"
    __testPath = "hdfs://192.168.1.33:9000/testdata.csv"
    def __init__(self, log, customSparkContext = None):
        self.logger = log
        self.logger.log("info","Initializing Naive Bayes..\n")
        self.logger.log("info","Initializing Spark..\n")

        if customSparkContext == None:
            conf = SparkConf()
            conf.setAppName("model-NaiveBayes")
            conf.setMaster("spark://192.168.1.33:7077")
            conf.set("spark.sql.warehouse.dir", "file:///C:\spark_warehouse")
            conf.set("spark.executor.memory","3G")
            conf.set("spark.driver.memory","3G")
            self.__sc = SparkContext(conf=conf)
            self.__sc.setLogLevel("ERROR")
            fs = (self.__sc._jvm.org
                  .apache.hadoop
                  .fs.FileSystem
                  .get(self.__sc._jsc.hadoopConfiguration())
                  )

        else:
            self.logger.log("info","Using custom SparkContext.. Skipping.. ")
            self.__sc = customSparkContext
            self.loadModelFromDisk()
        self.__SQLContext = SQLContext(self.__sc)
        self.logger.log("info","Complate..\n")

    def load_trainData(self):
        self.logger.log("info","Loading train and test data..\n")
        self.__trainData.loadData()
        self.__trainData.prepareText()
        self.__trainData.splitData()
        for p in self.__trainData.positive:
            self.__documents.append({"text": p, "label": 1})
        for p in self.__trainData.negative:
            self.__documents.append({"text": p, "label": 0})
        for p in self.__trainData.positive_test:
            self.__documents_test.append({"text": p, "label": 1})
        for p in self.__trainData.negative_test:
            self.__documents_test.append({"text": p, "label": 0})
        print("Complate..\n")

    def load_train(self):
       train_df = self.__SQLContext.read.csv(header=True, path=self.__trainPath)
       train_df = train_df.select("text", "sentiment").withColumnRenamed("text", "rawData").withColumnRenamed("sentiment","label")
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
        return  test_df

    def cleanTextOnDF(self, df : DataFrame):
        ct = CleanText()
        spark = self.__SQLContext
        df = ct.clean(dataFrame=df, spark=spark)
        return df
    def __preprocess_tdfidf(self,df:DataFrame):
        hashingTF = HashingTF().setInputCol("preprocessedData").setOutputCol("tf").setNumFeatures(1500000)
        idf = IDF().setInputCol("tf").setOutputCol("features")
        df = hashingTF.transform(df)
        df_model = idf.fit(df)
        df = df_model.transform(df)
        return df

    def preproccess(self, df:DataFrame):
        df = self.__preprocess_tdfidf(df)
        return df

    def loadModelFromDisk(self):
        self.logger.log("info","Loading pretrained model from disk \n")
        self.__model = NaiveBayesModel.load(self.__modelPath)
        self.logger.log("info","Complate \n")

    def train(self):
        self.logger.log("info","Training Model\n")
        raw_data = self.__sc.parallelize(self.__documents)
        raw_hashed_tf = raw_data.map(lambda dic: LabeledPoint(dic['label'], compTF(dic['text'])))
        raw_hashed_idf = compIDF(raw_hashed_tf)
        raw_hashed_tfidf = compTFIDF(raw_hashed_tf, raw_hashed_idf)
        self.__model = NaiveBayes.train(raw_hashed_tfidf)
        self.logger.log("info","Complate\n")

    def train2(self):
        print("Training Model\n")
        train_df = self.load_train()
        test_df = self.load_test()
        nb  = NaiveBayes()
        nb.setPredictionCol("predict_")
        nb.setFeaturesCol("features")
        nb.setLabelCol("label")
        self.__model = nb.fit(train_df)
        print("Complate\n")
        self.saveModel()
        self.testModel_df(test_df)

    def saveModel(self):
        if(self.__checkModel()):
            print("Saving model to disk\n")
            self.__model.write().overwrite().save(self.__modelPath)
            print("Complate\n")


    def __checkModel(self):
        if self.__model != None:
            return True
        else:
            raise Exception("No Model\n")
            return False



    def predict_df(self, dataFrame: DataFrame):
        __model: NaiveBayesModel = self.__model
        #df = self.cleanTextOnDF(dataFrame)
        df = self.__preprocess_tdfidf(dataFrame)
        df: DataFrame = __model.transform(df)
        df = df.withColumn("sentiment", df["predict_"]).drop("tf").drop("features").drop("rawPrediction").drop("probability").drop("predict_")
        return df

    def testModel_df(self, df: DataFrame):
        if(self.__checkModel()):
            print("Testing Model: \n")
            __model = self.__model
            NB_prediction_and_labels =  self.__model.transform(df)
            spark = self.__SQLContext
            NB_prediction_and_labels.registerTempTable("testData")
            NB_prediction_and_labels.show(100)
            NB_accuracy = spark.sql("SELECT count(label) FROM testData WHERE ((predict_ == 1.0 AND label == 4) OR (predict_ == 0.0 AND label == 0)) ").first()[0]  / spark.sql("SELECT count(label) FROM testData WHERE label != 2").first()[0]
            NB_precision = spark.sql("SELECT count(label) FROM testData WHERE (predict_ == 1.0 AND label == 4) ") .first()[0]  / spark.sql("SELECT count(label) FROM testData WHERE (predict_ == 1.0 AND label == 4) OR (predict_ == 1.0 AND label == 0)").first()[0]
            NB_recall = spark.sql("SELECT count(*) FROM testData WHERE (predict_ == 1.0 AND label == 4) ").first()[0]  / spark.sql("SELECT count(label) FROM testData WHERE (predict_ == 1.0 AND label == 4) OR (predict_ == 0.0 AND label == 4)").first()[0]
            f1_score = 2 * ((NB_precision * NB_recall) / (NB_precision + NB_recall))
            print("\t NB  Accuracy:" + str(NB_accuracy * 100) + " %" + "\n")
            print("\t NB  Precision:" + str(NB_precision * 100) + "\n")
            print("\tNB  Recall:" + str(NB_recall * 100) + "\n")
            print("\t NB  F1 Score:" + str(f1_score * 100) + "\n")

    def testModel(self):
        if(self.__checkModel()):
            print("Testing Model: \n")
            print("\n\n")
            __model = self.__model
            raw_data_test = self.__sc.parallelize(self.__documents_test)
            raw_hashed_test = raw_data_test.map(lambda dic:  (compTF(dic['text']), dic["label"]))
            NB_prediction_and_labels = raw_hashed_test.map(lambda point: (__model.predict(point[0]), point[1]))
            NB_correct = NB_prediction_and_labels.map(lambda row: decide(row))
            NB_accuracy = NB_correct.filter(lambda l: l == "TP" or l == "TN").count() / float(raw_hashed_test.count())
            NB_precision = NB_correct.filter(lambda l: l == "TP").count() / NB_correct.filter(lambda l: l == "TP" or l == "FP" ).count()
            NB_recall = NB_correct.filter(lambda l: l == "TP").count() / NB_correct.filter(lambda l: l == "TP" or l == "FN").count()
            f1_score = 2 * ((NB_precision * NB_recall) / (NB_precision + NB_recall))
            print("\t NB  Accuracy:" + str(NB_accuracy * 100) + " %" + "\n")
            print("\t NB  Precision:" + str(NB_precision * 100) + "\n")
            print("\tNB  Recall:" + str(NB_recall * 100) + "\n")
            print("\t NB  F1 Score:" + str(f1_score * 100) + "\n")

def decidedf(actual, predict):
    # (predict, actual)
    if actual == 4.0 and predict == 4.0:
        return "TP"
    elif actual == 0.0 and predict == 0.0:
        return "TN"
    elif actual == 0.0 and predict == 4.0:
        return "FP"
    elif actual == 4.0 and predict == 0.0:
        return "FN"
    else :
        return "other"

def decide(row):
    # (predict, actual)
    if row == (1.0, 1.0):
        return "TP"
    elif row == (0.0,0.0):
        return "TN"
    elif row == (1.0,0.0):
        return "FP"
    elif row == (0.0,1.0):
        return "FN"
    else :
        return row

def compTF(rdd):
    tf = HashingTF(150000)
    return tf.transform(rdd)
def compIDF(tf):
    tf.cache()
    idf = IDF().fit(tf)
    return idf
def compTFIDF(tf,idf):
    tfidf = idf.transform(tf)
    return tfidf
def convert_to_LabeledPoint(row):
    text = row["text"]
    label = row["label"]
    tf = compTF(text)
    idf = compIDF(tf)
    tf_idf = compTFIDF(tf, idf)
    return LabeledPoint(label,tf_idf)
