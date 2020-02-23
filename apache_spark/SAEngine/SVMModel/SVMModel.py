import inspect
import os
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from apache_spark.SAEngine.SAEngine import SAEngine
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import LinearSVCModel
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
import utils.naiveBayesPredict as nbp
from apache_spark.preprocess.CleanText import CleanText
class SVMModel(SAEngine):
    __modelPath = "hdfs://192.168.1.33:9000/LinearSVMModel.model"
    __trainPath = "hdfs://192.168.1.33:9000/trainingdata.csv"
    __testPath = "hdfs://192.168.1.33:9000/testdata.csv"
    def __init__(self, customSparkContext = None):
        print("Initializing Naive Bayes..\n")
        print("Initializing Spark..\n")

        if customSparkContext == None:
            conf = SparkConf()
            conf.setAppName("model-SVM")
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
            self.__sc = customSparkContext

        self.__SQLContext = SQLContext(self.__sc)
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
        print("Loading pretrained model from disk \n")
        self.__model = LinearSVCModel.load(self.__modelPath)
        print("Complate \n")



    def train2(self):
        print("Training Model\n")
        train_df = self.load_train()
        test_df = self.load_test()
        svm  = LinearSVC()
        svm.setPredictionCol("predict_")
        svm.setFeaturesCol("features")
        svm.setLabelCol("label")
        self.__model = svm.fit(train_df)
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
        sc = self.__sc
        _nbp = nbp.NaiveBayesPredict()
        _nbp.loadModel(sc)
        df = self.cleanTextOnDF(dataFrame)
        df = self.__preprocess_tdfidf(df)
        df = df.withColunmn("sentiment", _nbp.model.predict(df["features"]))
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
            print("\t SVM  Accuracy:" + str(NB_accuracy * 100) + " %" + "\n")
            print("\t SVM  Precision:" + str(NB_precision * 100) + "\n")
            print("\t SVM  Recall:" + str(NB_recall * 100) + "\n")
            print("\t SVM  F1 Score:" + str(f1_score * 100) + "\n")



sc = SVMModel()
sc.train2()