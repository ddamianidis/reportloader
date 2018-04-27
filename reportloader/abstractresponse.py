""" Defines the Response model """

import os
import re
import time
import requests
from reportloader.platforms.appnexus.reader import OutputReaderInterface   
from reportloader.platforms.appnexus.report import Report
from reportloader.interface.IResponseClient import IResponseClient   
            
class AbstractResponse():
    """ Class for manage an appnexus response """
            
    def dict(self):
        return self.__dict__
                
    def __getitem__(self, item):
        """ 
        Make the class subscriptable, so that someone can access the class
        attributes as in dicts.example response['placement_name']
        
        :param: the item/key to be accessed. 
        """
        
        return self.__dict__[item]
    
    def __setitem__(self, key, item):
        """ 
        Make the class subscriptable, so that someone can access the class
        attributes as in dicts.example response['placement_name']
        
        :param: the item/key to be accessed. 
        """
        
        self.__dict__[key] = item
     
    def __str__(self):
        """ 
        Returns the str representation of the object.
        
        :returns: the str representation of the object. 
        """
        
        return str(self.__dict__)
    
    def __repr__(self):
        """ 
        Returns the print representation of the object.
        
        :returns: the print representation of the object. 
        """
        
        return str(self.__dict__) 
        