from utils.textCleaner import textCleaner


class tweet_preprocess():
    def __init__(self):
        pass

    def preprocessTweet(self, tweet):
        tweet["text"] = self.__preprocessText(tweet["text"])
        return [tweet["created_at"], tweet["msg"]]

    def __preprocessText(self, text):
        tc = textCleaner()
        text = tc.preprocess(text)
        return text

