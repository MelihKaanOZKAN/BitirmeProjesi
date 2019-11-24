import re
import unicode2utf8
from nltk.tokenize import word_tokenize
from string import punctuation 
from nltk.corpus import stopwords 
import emoji
class textCleaner():
    
    def __init__(self):
        self._stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER','URL'])
       
    
    def preprocess(self, tweet):
        tweet = tweet.lower() # convert text to lower-case
        tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet) # remove URLs
        tweet = re.sub('@[^\s]+', 'AT_USER', tweet) # remove usernames
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet) # remove the # in #hashtag
        tweet = word_tokenize(tweet) # remove repeated characters (helloooooooo into hello)
        result = []
        tmpWordList = [word for word in tweet if word not in self._stopwords]
        for index, i in  enumerate(tmpWordList):
            tmp = self.cleanEmoji(i)
            if(i == 'rt'):
                continue
            if(i[0] == "'s"):
                tmpWordList[index-1].join(i)
                continue
            if(tmp != "" and tmp != ''):
                result.append(tmp)
        return result

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
    