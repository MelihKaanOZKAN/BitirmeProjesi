from cassandra.cluster import Cluster
import json, threading
class Adapter(threading.Thread):
    __cluster = Cluster(['134.122.116.110'])
    __session = None
    def __init__(self):
      self.__session =   self.__cluster.connect()
      self.__session.execute('USE db') #select keyspace
    def getSession(self):
      return self.__session
    
