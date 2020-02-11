class SAEngine():
    def __init__(self):
        pass
    def predict(self):
        raise Exception("You need to override this method")

    def train(self):
        raise Exception("You need to override this method")

    def loadModelFromDisk(self):
        raise Exception("You need to override this method")


