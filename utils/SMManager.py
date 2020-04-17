import subprocess, time, os, shlex, signal
from utils.InstructManager import InstructManager
class SMDict():
    __dict = {}
    class __obj(object):
        def __init__(self, subp):
            self.subp = subp

    def isEmptyId(self, sentimentId:str):
        try:
            a = self.__dict[sentimentId]
            return False
        except KeyError:
            return True
    def add(self, sentimentId, sm:subprocess):
        if(self.isEmptyId(sentimentId)):
            tmp = self.__obj(sm)
            self.__dict[sentimentId] = tmp
        else:
            raise KeyError
    def get(self, sentimentId) -> subprocess:
        if(self.isEmptyId(sentimentId)==False):
            return self.__dict[sentimentId].subp
        else:
            raise KeyError

class SMManager():
    def __init__(self):
        self.__smDict = SMDict()
        self.__IM = InstructManager()

    def startStream(self, sentimentId:str, mode:str, keywords:list):
        if self.__smDict.isEmptyId(sentimentId):
            #self.__IM.writeInstructions(sentimentId, mode, keywords, True)
            time.sleep(1.0)
            self.__createSpark(sentimentId)
        else:
            raise Exception("Error: sentimentId exist")
    def stopStream(self, sentimentId):
        if self.__smDict.isEmptyId(sentimentId) == False:
            sub = self.__smDict.get(sentimentId)
            time.sleep(15)
            os.kill(sub, signal.SIGINT)
        else:
            raise Exception("sentimentId doesnt exist")
    def isSparkContextRunning(self, sentimentId):
        if self.__smDict.isEmptyId(sentimentId) == False:
            pid = self.__smDict.get(sentimentId)
            import psutil
            proc = psutil.Process(pid)
            if (proc.status() == psutil.STATUS_RUNNING):
                return True
            else:
                return False
        else:
            raise Exception("Error. sentimentId doesnt exits")

    def __createSpark(self, sentimentId: str):
        pc = "python /Users/melihozkan/Desktop/Projects/BitirmeProjesi/sparkManager.py --host 192.168.1.62 --port 1998 --sentimentId " + sentimentId + "  --master spark://192.168.1.33:7077 --method naiveBayes"
        FNULL = open(os.devnull, 'w')
        DETACHED_PROCESS = 0x00000008
        sub = subprocess.Popen(shlex.split(pc))
        #sub = subprocess.Popen(shlex.split(pc), shell=False, stdin=FNULL, stdout=FNULL, stderr=FNULL, close_fds=True).pid
