from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import  json, csv
from datetime import datetime
import os,sys
sys.path.insert(0,'../../../..')
from utils.textCleaner import textCleaner
class CreateSample():
    now = datetime.now()
    here = os.path.dirname(os.path.abspath(__file__))
    filename_test = os.path.join(here, 'tweets_test_ ' + now.strftime("%d_%m_%Y-%H:%M:%S")+'.csv')   
    filename_train = os.path.join(here, 'tweets_train_ ' + now.strftime("%d_%m_%Y-%H:%M:%S")+'.csv')   
    def __init__(self, train_count, test_count):
        self.train = train_count
        self.test = test_count
        self.tc = textCleaner()
    
    def createTest(self, tweet):
        if(self.test != 0):
            ptext =  self.tc.preprocess(tweet)
            with open(self.filename_test, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(ptext)
            self.test = self.test - 1
        else:
            self.createTrain(tweet)
    def createTrain(self, tweet):
        if(self.train != 0):
            ptext =  self.tc.preprocess(tweet)
            with open(self.filename_train, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([tweet["id"], ptext])
            self.train = self.train - 1
        else:
            sys.exit(0)
    def withSpark(self):
        sc = SparkContext("local[2]", "TwitterSample")
        ssc = StreamingContext(sc, 1)
        socket = ssc.socketTextStream("127.0.0.1", 1998)
        lines = socket.window(1)
        ( 
            lines.flatMap(lambda line: line)
            .foreachRDD(lambda x: self.createTrain(x))
        )
        ssc.start()

CreateSample(600,100).withSpark()
        
    
   