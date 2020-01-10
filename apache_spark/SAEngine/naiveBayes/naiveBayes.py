from ..trainData import TrainData
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.classification import NaiveBayesModel
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext
class naiveBayes():
    trainData  = TrainData()
    def __init__(self):
        self.sc = SparkContext("local", "NaiveBayes")
        self.HashingTF = HashingTF(1000)
        filename = "/Users/melihozkan/Desktop/Projects/BitirmeProjesi/apache_spark/SAEngine/samples/AFINN.txt"
        self.AFINN = self.sc.textFile(filename)
        
