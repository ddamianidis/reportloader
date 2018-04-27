import MySQLdb
import MySQLdb.cursors
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import datetime
import simplejson
import configparser
import sys
import os
from reportloader.utils.mongoidtools import ObjectId
from reportloader.utils.config import Config

class MongoConnector():
        
    def __init__(self, collection=None):
        self.host = Config.getParam('mongo', 'host')    
        self.user = Config.getParam('mongo', 'user')
        self.password = Config.getParam('mongo', 'password')
        self.db = Config.getParam('mongo', 'db')
        self.port = Config.getParam('mongo', 'port')
        self.mongo_conn = MongoClient(self.host, int(self.port))
        self.mongo_db = getattr(self.mongo_conn, self.db)
        self.collection = collection
    
    @property
    def collection(self):
        return self.__collection
                
    @collection.setter
    def collection(self, collection):
        if collection:
            self.__collection = getattr(self.mongo_db, collection)
            
        else:
            self.__collection = None        
    
