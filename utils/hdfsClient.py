from  hdfs3 import HDFileSystem
class client():
    __client = None
    def __init__(self):
        self.__client = HDFileSystem(host='192.168.1.33', port=9000)
    def getClient(self):
        return self.__client
    def write(self, path, data):
        tmp = self.read(path)
        data = tmp + '\n' +  data
        with self.getClient().open(path, "ab") as f:
            f.write(data)
    def read(self, path):
        ret = ""
        try:
            with self.getClient().open(path) as f:
                ret = f.read()
        except OSError:
            return ""
        ret = str(ret, encoding="utf-8")
        return ret