import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel
import subprocess, time, os, shlex, signal, datetime

class tweepyServerModel(DjangoCassandraModel):
    serverid = columns.Integer(primary_key=True, default=uuid.uuid4)
    address = columns.Text()
    port = columns.SmallInt()


    def writeInstructs(self, sentimentId, mode, keywords):
        from utils.InstructManager import InstructManager
        im = InstructManager()
        return im.writeInstructions(sentimentId, mode, keywords)
    def generate_address(self):
        return self.address + ":" + str(self.port)

class sparkServerModel(DjangoCassandraModel):
    serverid = columns.Integer(primary_key=True, default=uuid.uuid4)
    address = columns.Text()
    port = columns.SmallInt()

    def generate_address(self):
        return "spark://" + self.address + ":" + str(self.port)


class sentiments(DjangoCassandraModel):

    sentimentid = columns.UUID(primary_key=True, default=uuid.uuid4())
    mode = columns.Text()
    keywords = columns.List(columns.Text)
    pids = columns.List(columns.Integer)
    createdate =  columns.DateTime()
    startdate = columns.DateTime()
    stopdate = columns.DateTime()
    status = columns.Text()
    notes = columns.List(columns.Text)
    sentimentname = columns.Text()
    lastupdate=columns.DateTime()

    def get_absolute_url(self):
        return "/sentimentManage/detail/{}".format(self.sentimentid)
    def get_start_url(self):
        return "/sentimentManage/start/{}".format(self.sentimentid)
    def get_stop_url(self):
        return "/sentimentManage/stop/{}".format(self.sentimentid)
    def check_none(self):
        if self.status == None:
           self.status= "New"



    def startSentiment(self):
        if self.status != "Running":
            tweepy: tweepyServerModel = tweepyServerModel.objects.get(serverid=1)
            tweepy.writeInstructs(self.sentimentid, self.mode, self.keywords)
            spark_pid =  self.__createSpark(self.sentimentid, tweepy)
            self.pids.append(spark_pid)
            dt = datetime.datetime.now()
            self.stopdate = dt
            self.lastupdate = dt
            self.status = "Running"
            self.save()

    def stopSentiment(self):
        if len(self.pids) > 0 and self.status == "Running":
            pid = self.pids[0]
            import os
            os.kill(pid, signal.SIGINT)
            dt = datetime.datetime.now()
            self.stopdate = dt
            self.lastupdate = dt
            self.status = "Stopped"
            self.save()

    def __createSpark(self, sentimentId: str, tweepy):
        spark:sparkServerModel = sparkServerModel.objects.get(serverid = 1)
        pc = "python /Users/melihozkan/Desktop/Projects/BitirmeProjesi/sparkManager.py --host {} --port {} --sentimentId {}  --master {} --method naiveBayes"
        cmd = (pc.format(tweepy.address, tweepy.port, self.sentimentid, spark.generate_address()))
        print("**********"  + cmd)
        FNULL = open(os.devnull, 'w')
        DETACHED_PROCESS = 0x00000008
        sub = subprocess.Popen(shlex.split(cmd)).pid
        return sub