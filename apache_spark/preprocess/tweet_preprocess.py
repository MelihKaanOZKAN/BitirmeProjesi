from utils.textCleaner import textCleaner


class tweet_preprocess():
    def __init__(self):
        pass

    def preprocessTweet(self, tweet):
        tweet["text"] = self.preprocessText(tweet["text"])
        return [tweet["created_at"], tweet["msg"]]

    def preprocessText(self, text):
        tc = textCleaner()
        text = tc.preprocess(text)
        return text

