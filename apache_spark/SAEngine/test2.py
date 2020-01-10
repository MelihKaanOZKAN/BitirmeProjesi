from sklearn.feature_extraction.text import CountVectorizer
from trainData import TrainData
from sklearn.ensemble import RandomForestClassifier


class test:
    def __init__(self):
        self.trainData = TrainData()
        self.trainData.loadData()
        self.trainData.prepareText()
        self.vectorizer = CountVectorizer(analyzer='word',
        tokenizer=None,
        preprocessor=None,
        stop_words=None,
        max_features=5000)
        self.trainDataFeatures= self.vectorizer.fit_transform(self.trainData.testSet).toArray()
        print(self.trainDataFeatures)

test()
