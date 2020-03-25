from cassandra.cluster import Cluster
import json
class Adapter():
    __cluster = Cluster(['134.122.116.110'])
    __session = None
    def __init__(self):
      self.__session =   self.__cluster.connect()
      self.__session.execute('USE sentiment_analysis') #select keyspace
    def getSession(self):
      return self.__session
    
