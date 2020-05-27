from django.test import TestCase, Client
from django.urls import reverse
from .models import *
import json
# Create your tests here.
class testViews(TestCase):

    def test(self):
        tweepyServer = tweepyServerModel()
        tweepyServer.serverid = 1
        tweepyServer.address = "192.168.1.62"
        tweepyServer.port = 1998
        tweepyServer.save()
        spark = sparkServerModel()
        spark.serverid = 1
        spark.port = 7077
        spark.address = "192.168.1.33"
        spark.save()
        print('default servers added to database')
        snt = sentiments()
        snt.notes = ['test']
        snt.keywords = ['news']
        snt.method = 'naiveBayes'
        snt.mode = 'filter'
        snt.status = 'new'
        snt.sentimentname = 'test'
        snt = snt.save()
        print('Analysis Created')
        b = snt.startSentiment()
        print('Analysis started. Waiting 80 seconds')
        import time
        time.sleep(80)
        snt.stopSentiment()
        print('Analysis stop signal sended')
        a = time.time()
        while True:
            snt.isSparkContextRunning()
            if snt.last_update_text != 'Yes':
                break
        b = time.time()
        total = float(b - a)
        print('Analysis Stopped in {} seconds'.format(total))