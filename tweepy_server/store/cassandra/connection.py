from cassandra.cluster import Cluster
import json
class Adapter():
    __cluster = Cluster(['127.0.0.1'])
    __session = None
    def __init__(self):
      self.__session =   self.__cluster.connect()
      self.__session.execute('USE Tweets') #select keyspace
    def getSession(self):
      return self.__session
    
