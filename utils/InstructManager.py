from utils.hdfsClient import client
class InstructManager():
    def __init__(self):
        self.__hdfs = client()
    __instructPath =   '/instructions.txt'

    def writeInstructions(self, sentimentId: str, mode : str, keywords : list, overwrite=False):
        write = "--ID\n" + sentimentId
        if mode == "filter":
            write += "\n--filter\n"
        else:
            print("wrong mode")
        for index, i in enumerate(keywords):
            write += i
            if (index < len(keywords) -1):
                write += "\n"
        if overwrite:
           self.__writeToFile_overwrite(write)
        else:
            self.__writeToFile(write)
        return True
    def __writeToFile(self, text):
         self.__hdfs.write(self.__instructPath,text)

    def __writeToFile_overwrite(self, text):
        self.__hdfs.overwrite(self.__instructPath, text)
    def readInstruct(self):
        return self.__hdfs.read('/instructions.txt')


