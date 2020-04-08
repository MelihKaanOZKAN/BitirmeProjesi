import subprocess
class SMManager():
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