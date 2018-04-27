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

    def getToken(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError

class Account(AccountInterface):
    """ Class responsible to manage appnexus api account """
    
    def __init__(self, account_id=None, credentials=None):
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.account_id =  account_id if account_id else 'tdg-network' 
        self.client_id =  Config.getParam('taboola', 'client_id')
        self.client_secret = Config.getParam('taboola', 'client_secret')
    
    def getCurrentFilePath(self, filename):
        here = os.path.dirname(os.path.realpath(__file__))
        file_full_path = os.path.join(here, filename)
        return file_full_path
        
    def getToken(self):
        """ 
        Get the access token which gives the access to api calls 
         
        :returns: returns the access token or False on error.
        """
            
        # Get access token
        data = 'client_id={0}&client_secret={1}&grant_type=client_credentials'\
                .format(self.client_id, self.client_secret)
    
        headers = {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    
        r = requests.post('https://backstage.taboola.com/backstage/oauth/token', data=data, headers=headers)
        if r.status_code != 200:
            self.stream_logger.error('Invalid access token, status:{0}'.format(r.status_code))
            return False
        else:
            response_data = r.json()
    
        access_token = response_data['access_token']
        
        return access_token 
                