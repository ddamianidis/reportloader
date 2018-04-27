""" Module for retrieving appnexus reporting data """    
import os
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client import file, client, tools
from apiclient.discovery import build
import imaplib
import email
from httplib2 import Http
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
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.gmail = Config.getParam('rubicon', 'gmail')
        self.username = Config.getParam('rubicon', 'username')
        self.password = Config.getParam('rubicon', 'password')
        self.client_id =  Config.getParam('rubicon', 'gmail_client_id')
        self.client_secret = Config.getParam('rubicon', 'gmail_client_secret')
        self.scope='https://www.googleapis.com/auth/gmail.readonly'
        self.credentials_file = self.getCurrentFilePath('data/gmail_credentials.json')
                
        store = file.Storage(self.credentials_file)
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(self.credentials_file, self.scope)
            creds = tools.run_flow(flow, store)
        self.mail_connection = build('gmail', 'v1', http=creds.authorize(Http()))
        
        
    def getCurrentFilePath(self, filename):
        here = os.path.dirname(os.path.realpath(__file__))
        file_full_path = os.path.join(here, filename)
        return file_full_path                