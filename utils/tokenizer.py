from word2vecExt import word2vecExt

class tokenizer():
    def __init__(self):
        self.word2vec = word2vecExt()
        self.tokenizer = Tokenizer(num_words=len(TRAINING_VOCAB), lower=True, char_level=False)
    def tokenize(self, word):
        pass