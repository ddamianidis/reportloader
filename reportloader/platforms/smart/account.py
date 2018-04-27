""" Module for retrieving appnexus reporting data """    
import requests
from reportloader.utils.config import Config
       
class AccountInterface():
    """The 'AccountInterface' class declares the interface that must be
    implemented by all account.
    """

    def getToken(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError

class Account(AccountInterface):
    """ Class responsible to manage appnexus api account """
    
    def __init__(self):
        self.networkid = Config.getParam('smart', 'networkid')    
        self.username = Config.getParam('smart', 'username')
        self.password = Config.getParam('smart', 'password')            