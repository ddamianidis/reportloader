""" Module for retrieving appnexus reporting data """    
import requests
import os
import httplib2
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client import tools
from reportloader.utils.config import Config
from reportloader.utils.logger import StreamLogger
       
class AccountInterface():
    """The 'AccountInterface' class declares the interface that must be
    implemented by all account.
    """

    def authenticate(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError

class Account(AccountInterface):
    """ Class responsible to manage appnexus api account """
    
    def __init__(self):
        self.stream_logger = StreamLogger.getLogger(__name__)
    
    def getCurrentFilePath(self, filename):
        here = os.path.dirname(os.path.realpath(__file__))
        file_full_path = os.path.join(here, filename)
        return file_full_path
        
    def authenticate(self):
        """ 
        Get the access token which gives the access to api calls 
         
        :returns: returns the access token or False on error.
        """
            
        # First time credential approval requires manual intervention
        
        if not os.path.exists(self.credentials_file):
            self.stream_logger.error('Invalid credential file path')
            return False
    
        # Create authorization flow
        flow = OAuth2WebServerFlow(self.client_id, self.client_secret, 
                                   self.scope)
        storage = Storage(self.credentials_file)
        credentials = storage.get()
        if credentials is None or credentials.invalid:
            credentials = tools.run_flow(flow, storage, tools.argparser.parse_args())
    
        http = httplib2.Http()
        http = credentials.authorize(http)

        return http

class AdsenseAccount(Account):
    
    def __init__(self, account_id=None, credentials=None):
        super().__init__()
        self.account_id =  account_id if account_id else 'pub-5233202974419683' 
        self.client_id =  Config.getParam('adsense', 'client_id')
        self.client_secret = Config.getParam('adsense', 'client_secret')
        self.scope='https://www.googleapis.com/auth/adsense.readonly'
        self.credentials_file = \
        credentials if credentials else self.getCurrentFilePath('data/adsense_credentials.dat')

class AdxAccount(Account):
    
    def __init__(self, account_id=None, credentials=None):
        super().__init__()
        self.account_id =  account_id if account_id else 'pub-2500372977609723' 
        self.client_id =  Config.getParam('adx', 'client_id')
        self.client_secret = Config.getParam('adx', 'client_secret')
        self.scope='https://www.googleapis.com/auth/adexchange.seller.readonly'
        self.credentials_file = \
        credentials if credentials else self.getCurrentFilePath('data/adx_credentials.json')
