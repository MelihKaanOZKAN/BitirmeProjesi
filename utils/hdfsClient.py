from hdfs3 import core
class client():
    __client = None
    def __init__(self):
        cr = core
        cr.logger.disabled = True

        #self.__client = cr.HDFileSystem(host='192.168.1.33', port=9000) TODO Fix this
        self.__client = None
    def getClient(self):
        return self.__client
    def write(self, path, data):
        return True
        tmp = self.read(path)
        data = tmp + '\n' +  data
        with self.getClient().open(path, "ab") as f:
            f.write(data)
    def overwrite(self, path, data):
        return  True
        with self.getClient().open(path, "wb") as f:
            f.write(data)
    def read(self, path):
        ret = "SAMPLE LOG"
        return ret
        try:
            with self.getClient().open(path) as f:
                ret = f.read()
        except OSError as e:
            ret=  e
        ret = str(ret, encoding="utf-8")
        return ret
