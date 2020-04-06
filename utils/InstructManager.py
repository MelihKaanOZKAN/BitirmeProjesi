from utils.hdfsClient import client
class InstructManager():
    def __init__(self):
        self.__hdfs = client()
    def WriteInstructions(self, sentimentId: str, mode : str, keywords : list):
        write = "--ID \n" + sentimentId
        if mode == "filter":
            write += "\n--filter \n"
        else:
            print("wrong mode")
        for index, i in enumerate(keywords):
            write += i
            if (index < len(keywords) - 1):
                write += "\n"
        self.__writeToFile(write)
        print("instruct writed")

    def __writeToFile(self, text):
         self.__hdfs.write('/instructions.txt',text)
    def readInstruct(self):
        return self.__hdfs.read('/instructions.txt')
