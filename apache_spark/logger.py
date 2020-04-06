import datetime, threading
from utils.hdfsClient import client
def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance
class __logger():
    __sentimentId = None
    def __init__(self):
        self.__hdfs = client()
    def __getFileName(self):
        return  "/logs/sentimentlog_" + self.__sentimentId + ".log"
    def __getDateTime(self):
        dt = datetime.datetime.now()
        return str(dt.year) + "-" + str(dt.month) +"-" + str(dt.day) + " " + str(dt.hour) + ":" + str(dt.minute) + ":" + str(dt.second) +"," + str(dt.microsecond)
    def createLog(self, sentimentId):
        self.__sentimentId = sentimentId
        write = "LOG CREATE TIME: " + self.__getDateTime() + " SentimentId: " + sentimentId
        self.__writeToFile(write)
    def __writeToFile(self,  text):
        filename = self.__getFileName()
        self.__hdfs.write(filename, text)

    def log(self, mode:str, message:str):
        write = "[" + mode + "] " + self.__getDateTime() + " - " + message
        self.__writeToFile(write)
@singleton
class logger(__logger):
    pass