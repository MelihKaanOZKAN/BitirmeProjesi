
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
import os
class modelLoader():

    def loadModelFromDisk(self, sc):
        print("Loading pretrained model from disk \n")
        model = NaiveBayesModel.load(sc, "hdfs://192.168.1.33:9000//NaiveBayes.model")
        print("Complate \n")
        return model