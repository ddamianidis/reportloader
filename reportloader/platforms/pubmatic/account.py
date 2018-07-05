""" Module for retrieving appnexus reporting data """    
import requests
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
    
    def __init__(self):
        self.username = Config.getParam('pubmatic', 'username')    
        self.password = Config.getParam('pubmatic', 'password')
        self.app_token = Config.getParam('pubmatic', 'app_token')
        self.grant_type = Config.getParam('pubmatic', 'grant_type')
        self.stream_logger = StreamLogger.getLogger(__name__)
        
    def getToken(self):
        data = {
            "username":self.username,
            "password": self.password,
            "grant_type":self.grant_type    
               
            }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'api.pubmatic.com',
            'Authorization': 'Basic {0}'.format(self.app_token)
            #'Authorization': 'Basic dzlxMGxMTDJRb3laWXVzZWpTRUR5VDlHSFVrZmFUclU6R0FGcEFVZ1R1TFd1QThiUA=='
        }
    
        r = requests.post('http://api.pubmatic.com/v1/oauth/generateOauthToken', data=data, headers=headers)
        
        if r.status_code != 200:
            self.stream_logger.error('Error while retrieving access token')
            self.stream_logger.error('Status code {0}'.format(r.status_code))
            return False

        ac_data = r.json()
        access_token = ac_data['access_token'] 
        self.stream_logger.debug('access_token: {0}'.format(access_token))
        return access_token
                