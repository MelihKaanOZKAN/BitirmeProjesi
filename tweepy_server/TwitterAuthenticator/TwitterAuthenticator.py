from tweepy import OAuthHandler
import json
import os
class TwitterAuthenticator():
    def __init__(self):
        here = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(here, 'twitterCredentials.json')
        with open(filename,"r", encoding="utf-8") as data:
            dataTmp = json.loads(data.read())
            self.CONSUMER_KEY = dataTmp["CONSUMER_KEY"]
            self.CONSOMER_KEY_SECRET = dataTmp["CONSOMER_KEY_SECRET"]
            self.ACCESS_TOKEN = dataTmp["ACCESS_TOKEN"]
            self.ACCESS_TOKEN_SECRET = dataTmp["ACCESS_TOKEN_SECRET"]
    def getAuth(self):
        auth = OAuthHandler(self.CONSUMER_KEY, self.CONSOMER_KEY_SECRET)
        auth.set_access_token(self.ACCESS_TOKEN, self.ACCESS_TOKEN_SECRET)
        return auth