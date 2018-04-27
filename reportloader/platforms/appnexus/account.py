""" Module for retrieving appnexus reporting data """    
import requests
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
    
    def __init__(self, username='TW_PA_Admin', password='9^gSpg1111',
                 auth_url='https://api.appnexus.com/auth'):
        self.username = username
        self.password = password
        self.auth_url = auth_url
        self.stream_logger = StreamLogger.getLogger(__name__) 
    
    def getToken(self):
        """ 
        Get the access token which gives the access to api calls 
         
        :returns: returns the access token or False on error.
        """
        
        data = '''
                {
                "auth": 
                    {
                        "username" : "%s",
                        "password" : "%s"
                    }
                }
            ''' % (self.username, self.password)
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'api.appnexus.com'
        }
        r = requests.post(self.auth_url, data=data, 
                          headers=headers)
        ac_data = r.json()
        
        if ac_data['response']['status'] != 'OK':
            self.stream_logger.error('Error while retrieving access token')
            self.stream_logger.error('Status code {0}'\
                                     .format(ac_data['response']['status']))
            return False

        return ac_data['response']['token']
                