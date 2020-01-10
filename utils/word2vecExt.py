import gensim, os
class word2vecExt():
  __positive = []
  __negative = []
  def __init__(self):
   self.__model = None
   self.__loadPosNeg()

  def __correctWords(self, wordList):
    """Corrects words in wordList
    
    Parameters:
    wordList: words to be corrected

    Return:
    list:Corrected words
    
    """
    for index, i in enumerate(wordList):
      wordList[index] =  str(i).rstrip('\n')
    return wordList

  def __loadPosNeg(self):
    """Loads positive and negative words from text files
    """
    try:
      print("Loading words..")
      fileDir = os.path.dirname(os.path.realpath(__file__))
      filename = os.path.join(fileDir, 'wordLists/positive-words_2.txt')
      with open(filename, mode='r', encoding='utf-8') as f:
        self.__positive = f.readlines()
      filename = os.path.join(fileDir, 'wordLists/negative-words_2.txt')
      with open(filename, mode='r', encoding='utf-8') as f:
        self.__negative = f.readlines()
      self.__positive  = self.__correctWords(self.__positive)
      self.__negative = self.__correctWords(self.__negative)
      print("Words loaded..")
    except FileNotFoundError:
      raise FileNotFoundError("Words lists not found.")

  def load_model(self):
    """Loads word2vec model from file
    """
    try:
      print('Loading model..')
      fileDir = os.path.dirname(os.path.realpath(__file__))
      filename = os.path.join(fileDir, 'models/GoogleNews-vectors-negative300.bin')
      self.__model = gensim.models.KeyedVectors.load_word2vec_format(filename, binary=True)
      print("Model loaded..")
    except:
      raise FileNotFoundError("Model file not found")

  def __checkModel(self):
    """Checks if model loaded
    """
    if(self.__model == None):
      raise Exception("Model not loaded. Try load_model() first?")
  def __checkWords(self, words):
    """Checks if words are string
    """
    if ( type(words) == list):
      for i in words:
        if(type(i) != str):
          raise Exception("Non-string value found..")
    else:
      raise Exception("This is not a list")
  def getWordVector(self, word):
    """Vectorize a word
    
    Parameters:
    word: a word to be vectorized

    Return:
    Vector : vector as list


    """
    self.__checkModel()
    self.__checkWords([word])
    return self.__model[word]
    
  def similarity(self, word1, word2):
    """Calculates similarity of two words
    
    Parameters:
    word1: a word 
    word2: othor word


    Return:
    float: Similarity


    """
    self.__checkModel()
    self.__checkWords([word1, word2])
    return self.__model.similarity(word1, word2)

  def positiveNegative(self,word):
     """Calculates positive and negative ratios
    
    Parameters:
    word: a word 


    Return:
    list: [positive-ratio, negative-ratio]


    """
    self.__checkModel()
    self.__chechWords([word])
    try:
      positive =  self.__model.n_similarity(self.__positive, [word])
      negative =  self.__model.n_similarity(self.__negative, [word])
      return [positive, negative]
    except:
      KeyError("word not in vocabulary")
  def __test(self):
    invocabp = []
    invocabn = []
    for i in self.__positive:
      try:
        a = self.__model[i]
        invocabp.append(i)
      except KeyError:
        print("")
    for i in self.__negative:
      try:
        a = self.__model[i]
        invocabn.append(i)
      except KeyError:
        print("")
    fileDir = os.path.dirname(os.path.realpath(__file__))
    filename = os.path.join(fileDir, 'wordLists/positive-words_2.txt')
    with open(filename, mode="a", encoding="utf-8") as f:
      for i in invocabp:
        f.write(i)
        f.write('\n')
    filename = os.path.join(fileDir, 'wordLists/negative-words_2.txt')
    with open(filename, mode="a", encoding="utf-8") as f:
      for i in invocabn:
        f.write(i)
        f.write('\n')
