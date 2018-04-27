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
    
    def __init__(self, app):
        self.name = Config.getParam(app, 'name')    
        self.id = Config.getParam(app, 'id')
        self.secret = Config.getParam(app, 'secret')
        self.stream_logger = StreamLogger.getLogger(__name__)
        
    def getToken(self):
        
        r = requests.get('https://graph.facebook.com/oauth/access_token?'
                         'client_id={0}&client_secret={1}&'
                         'grant_type=client_credentials'.format(self.id, 
                                                                self.secret))
        if r.status_code != 200:
            self.stream_logger.error('Error code {0}'.format(r.status_code))
            return None
        '''m = re.match(r'^access_token=(.*)$', r.text)
        if m is None:
            rootLogger.error('Regex for access token failed')
            return None

        access_token = m.group(1)'''
        ac_data = r.json()
        access_token = ac_data['access_token'] 
        return access_token

                