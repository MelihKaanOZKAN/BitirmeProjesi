import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel
import subprocess, time, os, shlex, signal, datetime

class tweepyServerModel(DjangoCassandraModel):
    serverid = columns.Integer(primary_key=True, default=uuid.uuid4)
    address = columns.Text()
    port = columns.SmallInt()


    def writeInstructs(self, sentimentId, mode, keywords):
        import sys
        sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
        from  utils.InstructManager import InstructManager
        im = InstructManager()
        return im.writeInstructions(sentimentId, mode, keywords, True)
    def writeStopFile(self, sentimentId):
        import sys
        sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
        from utils.hdfsClient import client
        tmp = client()
        tmp.overwrite(path="/tweepy/" + str(sentimentId) + "_stop.txt", data="test")

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
    method = columns.Text()
    last_update_text = ""

    def get_absolute_url(self):
        return "/sentimentManage/detail/{}".format(self.sentimentid)
    def get_report_url(self):
        return "/reportService/index/{}".format(self.sentimentid)
    def get_start_url(self):
        return "/sentimentManage/start/{}".format(self.sentimentid)
    def get_stop_url(self):
        return "/sentimentManage/stop/{}".format(self.sentimentid)
    def get_log_url(self):
        return "/sentimentManage/log/{}".format(self.sentimentid)
    def check_none(self):
        if self.status == None:
           self.status= "New"
    def get_log_path(self):
        return "/logs/sentimentlog_" + str(self.sentimentid) + ".log"
    def startSentiment(self):
        if len(self.pids) == 0:
            tweepy: tweepyServerModel = tweepyServerModel.objects.get(serverid=1)
            tweepy.writeInstructs(self.sentimentid, self.mode, self.keywords)
            tweepy.writeStopFile(self.sentimentid)
            spark_pid =  self.__createSpark(self.sentimentid, tweepy)
            self.pids.append(spark_pid)
            dt = datetime.datetime.now()
            self.startdate = dt
            self.lastupdate = dt
            self.status = "Running"
            self.save()
            return True
        else:
            return False
    def stopSentiment(self):
        if len(self.pids) > 0 and self.status == "Running":
            pid = self.pids[0]
            import os, psutil
            if psutil.pid_exists(pid):
                os.kill(pid, signal.SIGUSR1)
                dt = datetime.datetime.now()
                self.stopdate = dt
                self.lastupdate = dt
                self.status = "Stop signal"
                self.save()
                return True
            else:
                self.pids.clear()
                self.status = "Stopped"
                self.save()
                return False
    def __createSpark(self, tweepy):
        spark:sparkServerModel = sparkServerModel.objects.get(serverid = 1)
        pc = "python /Users/melihozkan/Desktop/Projects/BitirmeProjesi/sparkManager.py --host {} --port {} --sentimentId {}  --master {} --method {}"
        cmd = (pc.format(tweepy.address, tweepy.port, self.sentimentid, spark.generate_address(), self.method))
        print(cmd)
        FNULL = open(os.devnull, 'w')
        DETACHED_PROCESS = 0x00000008
        sub = subprocess.Popen(shlex.split(cmd), stderr=FNULL, stdout=FNULL)
        return sub.pid
    def checkPids(self, noPid):
        if len(self.pids) > 0:
            pid = self.pids[0]
            try:
                import psutil
                proc = psutil.Process(pid)
                if (proc.status() == psutil.STATUS_RUNNING):
                    self.last_update_text = "Yes"
                else:
                    self.pids.clear()
                    if self.status == "Running":
                        self.status = noPid
                        dt = datetime.datetime.now()
                        self.lastupdate = dt
                    self.save()
                    self.last_update_text = noPid
            except psutil.NoSuchProcess:
                self.pids.clear()
                if self.status == "Running":
                    self.status = noPid
                    dt = datetime.datetime.now()
                    self.lastupdate = dt
                self.save()
                self.last_update_text = noPid
        else:
            self.last_update_text = noPid
    def isSparkContextRunning(self):
        if self.status == "Running":
            self.checkPids("DEAD")
        elif self.status == "Stop signal":
            self.checkPids("Stopped")
        elif self.status == "NEW":
            self.last_update_text =  'No Context'
        else:
            self.pids.clear()
            self.save()
            self.last_update_text =  'Process not found'