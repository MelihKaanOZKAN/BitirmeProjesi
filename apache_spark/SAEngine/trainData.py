import csv, os, math, sys
sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi')
from utils.textCleaner import textCleaner

class TrainData():
    trainRatio = 80
    trainSet = []
    testSet = []
    trainLabel = []
    testLabel=  []
    positive = []
    neutral = []
    negative = []
    fileDir = os.path.dirname(os.path.realpath(__file__))
    def __init__(self, dataset_train = "training.1600000.processed.noemoticon.csv.nosync", dataset_test  = "testdata.manual.2009.06.14.csv.nosync"):
        self.dataset_train = dataset_train
        self.dataset_test = dataset_test
        self.cleaner = textCleaner()

   
    
    def rowCount(self, dataset):
        result = 0
        filename = os.path.join(self.fileDir, 'samples/'+dataset)
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                print(row)
                if line_count == 0:
                    line_count += 1
                else:
                    result += 1
                    line_count += 1
        return result

    def loadTrain(self):
        filename = os.path.join(self.fileDir, 'samples/'+self.dataset_train)
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    self.trainSet.append(row[5])
                    self.trainLabel.append(row[0])
                    line_count += 1
    def loadTest(self):
        filename = os.path.join(self.fileDir, 'samples/'+self.dataset_test)
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                else:
                    self.testSet.append(row[5])
                    self.testLabel.append(row[0])
                    line_count += 1
    def loadData(self):
        """totalRow_train = self.rowCount(self.dataset_train)
        totalRow_test = self.rowCount(self.dataset_test)
        totalrow = totalRow_test + totalRow_train
        print("Loading Dataset... Train:")
        print("Total Set: {} \nTrain Set: {}\nTest Set: {}".format(totalrow, totalRow_train, totalRow_test))"""
        #self.loadTrain()
        self.loadTest()
        print("Dataset Loaded...")

    def prepareText(self):
        print("textPrepare..")
        newTrainSet = []
        newTestSet = []
        for  i in self.trainSet:
            newTrainSet.append(self.cleaner.preprocess(i))
        for  i in self.testSet:
            k = self.cleaner.preprocess(i)
            newTestSet.append(k)
        self.testSet = newTestSet
        self.trainSet = newTrainSet
        print("textPrepare done")
    def strList(self, list):
        res = ""
        for index,i  in enumerate(list):
            if(index+1 < len(list)):
                res += i + " "
            else:
                res += i
        return res

    def splitData(self):
        for index, i in enumerate(self.trainLabel):
            if(i == "0"):
                self.negative.append(self.trainSet[index])
            if(i == "2"):
                self.neutral.append(self.trainSet[index])
            if(i == "4"):
                self.positive.append(self.trainSet[index])
    def saveDataAsTextFile(self):
        filename = os.path.join(self.fileDir, 'samples/'+self.dataset_train+"_positive.txt")
        with open(filename, mode="a") as file:
            for  i in self.positive:
                file.write(self.strList(i))
                file.write("\n")
        filename = os.path.join(self.fileDir, 'samples/'+self.dataset_test+"_neutral.txt")
        with open(filename, mode="a") as file:
            for  i in self.neutral:
                file.write(self.strList(i))
                file.write("\n")
        filename = os.path.join(self.fileDir, 'samples/'+self.dataset_test+"_negative.txt")
        with open(filename, mode="a") as file:
            for i in self.negative:
                file.write(self.strList(i))
                file.write("\n")