""" Module for retrieving appnexus reporting data """    
import requests
       
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
        pass
    
    def getToken(self):
        """ 
        Get the access token which gives the access to api calls 
         
        :returns: returns the access token or False on error.
        """
        
        access_token = "b18d96ee0a9c455db41f13dabc04c8a2"
        return access_token  
                