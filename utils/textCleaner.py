import re
from .unicode2utf8 import unicode2utf8
from nltk.tokenize import word_tokenize
from string import punctuation 
from nltk.corpus import stopwords 
import emoji
from pyspark.sql import DataFrame
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline


class textCleaner():
    
    def __init__(self):
        self._stopwords = set(stopwords.words('english'))
        
       
    
    def preprocess(self, tweet):
        tweet = tweet.lower() # convert text to lower-case
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', '', tweet) # remove URLs
        tweet = re.sub('@[^\s]+', '', tweet) # remove usernames
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet) # remove the # in #hashtag
        negations_dic = {"isn't":"is not", "aren't":"are not", "wasn't":"was not", "weren't":"were not",
                 "haven't":"have not","hasn't":"has not","hadn't":"had not","won't":"will not",
                 "wouldn't":"would not", "don't":"do not", "doesn't":"does not","didn't":"did not",
                 "can't":"can not","couldn't":"could not","shouldn't":"should not","mightn't":"might not",
                 "mustn't":"must not"}
        neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')
        tweet = neg_pattern.sub(lambda x: negations_dic[x.group()], tweet)
        tweet = re.sub("[^a-zA-Z]", " ", tweet)
        tweet = word_tokenize(tweet) # remove repeated characters (helloooooooo into hello)
        result = []
        tmpWordList = [word for word in tweet if word not in self._stopwords and word != "rt"]
        for index, i in  enumerate(tmpWordList):
            tmp = self.cleanEmoji(i)
            if(self.filter(tmp)):
                    tmp = tmp.replace("\n", "")
                    tmp = tmp.replace("\t", "")
                    result.append(tmp)
        return result

    def preproccess_train_spark(self, train_set, val_set, dataFrame: DataFrame):
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashtf = HashingTF(numFeatures=2 ** 16, inputCol="words", outputCol='tf')
        idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5)  # minDocFreq: remove sparse terms
        label_stringIdx = StringIndexer(inputCol="target", outputCol="label")
        pipeline = Pipeline(stages=[tokenizer, hashtf, idf, label_stringIdx])
        pipelineFit = pipeline.fit(train_set)
        train_df = pipelineFit.transform(train_set)
        val_df = pipelineFit.transform(val_set)
        return train_df, val_df

    def filter(self, text):
        res = True
        if(text == "" ):
            res = False
        if(text == '' ):
            res = False
        if(text == "," ):
            res = False
        if(text == "." ):
            res = False
        if(text == "..." ):
            res = False
        if(text == "$" ):
            res = False
        return res
    def cleanEmoji(self, text):
        result = []
        for character in text:
            if character in emoji.UNICODE_EMOJI:
                pass
            else:
                result.append(character)
        return ''.join(result)


    

    def unicode2tutf8(self, text):
        u2utf8 = unicode2utf8.unicode2utf8()
        return u2utf8.convert(text)
    