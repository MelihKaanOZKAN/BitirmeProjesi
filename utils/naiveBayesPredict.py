
from pyspark import SparkConf, SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.classification import  NaiveBayesModel
model:NaiveBayesModel
class NaiveBayesPredict:
    model = NaiveBayesModel
    def loadModel(self, sc):
        global model
        model = NaiveBayesModel.load(sc, "hdfs://192.168.1.33:9000/NaiveBayes.model")
    def predict(self, text):
        global model
        tf = self.compTF(text)
        return model.predict(tf)
    def compTF(self, rdd):
        tf = HashingTF(150000)
        return tf.transform(rdd)

    def compIDF(tf):
        # tf.cache()
        idf = IDF().fit(tf)
        return idf

    def compTFIDF(tf, idf):
        tfidf = idf.transform(tf)
        return tfidf