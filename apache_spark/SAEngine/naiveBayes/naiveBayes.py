import inspect
import os
import sys

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from trainData import TrainData
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.sql import DataFrame

class naiveBayes():
    __trainData  = TrainData()
    __documents = []
    __documents_test = []

    def __init__(self, customSparkContext = None):
        print("Initializing Naive Bayes..")
        print("Initializing Spark..")

        if (customSparkContext == None):
            conf = SparkConf()
            conf.setAppName("NaiveBayes")
            conf.setMaster("local[*]")
            self.__sc = SparkContext(conf=conf)
            self.__sc.setLogLevel("INFO")
        else:
            self.__sc = customSparkContext
        print("Complate..")

    def load_trainData(self):
        print("Loading train and test data..")
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
        print("Complate..")

    def loadModelFromDisk(self):
        print("Loading pretrained model from disk")
        self.__model = NaiveBayesModel.load(self.__sc, "NaiveBayes.model")
        print("Complate")

    def train(self):
        print("Training Model")
        raw_data = self.__sc.parallelize(self.__documents)
        raw_hashed = raw_data.map(lambda dic: LabeledPoint(dic['label'], compTF(dic['text'])))
        self.__model = NaiveBayes.train(raw_hashed)
        print("Complate")

    def saveModel(self):
        if(self.__checkModel()):
            print("Saving model to disk")
            self.__model.save(self.__sc, "NaiveBayes.model")
            print("Complate")

    def __checkModel(self):
        if self.__model != None:
            return True
        else:
            raise Exception("No Model")
            return False

    def predict(self, dataFrame: DataFrame):
        if (self.__checkModel()):
            dataFrame.withColumn("features", dataFrame['preprocessedData'])
            dataFrame.withColumn("sentiment", self.__model.predict(dataFrame["features"]))
            return dataFrame


    def testModel(self):
        if(self.__checkModel()):
            print("Testing Model: ")
            print("\n")
            raw_data_test = self.__sc.parallelize(self.__documents_test)
            raw_hashed_test = raw_data_test.map(lambda dic: LabeledPoint(dic['label'], compTF(dic['text'])))
            NB_prediction_and_labels = raw_hashed_test.map(lambda point: (self.__model.predict(point.features), point.label))
            NB_correct = NB_prediction_and_labels.filter(lambda row: decide(row))
            print(NB_correct.take(1000))
            NB_accuracy = NB_correct.filter(lambda l: l == "TP").count() / float(raw_hashed_test.count())
            NB_precision = NB_correct.filter(lambda l: l == "TP").count() / NB_correct.filter(lambda l: l == "TP" or l == "FP" ).count()
            NB_recall = NB_correct.filter(lambda l: l == "TP").count() / NB_correct.filter(lambda l: l == "TP" or l == "FN").count()
            f1_score = 2 * ((NB_precision * NB_recall) / (NB_precision + NB_recall))
            print("\t NB  Accuracy:" + str(NB_accuracy * 100) + " %" + "\n")
            print("\t NB  Presicion:" + str(NB_precision * 100) + "\n")
            print("\tNB  Recall:" + str(NB_recall * 100) + "\n")
            print("\t NB  F1 Score:" + str(f1_score * 100) + "\n")
        else:
            raise Exception("No Model")

def decide(row):
    if(row[0] == 1 and row[1] == 1):
        return "TP"
    elif (row[0] == 0 and row[1] == 0):
        return "TN"
    elif (row[0] == 1 and row[1] == 0):
        return "FP"
    elif (row[0] == 0 and row[1] == 1):
        return "FN"
    else :
        return row

def compTF(rdd):
    tf = HashingTF(150000)
    return tf.transform(rdd)


sc = naiveBayes()
sc.load_trainData()
sc.loadModelFromDisk()
sc.testModel()

