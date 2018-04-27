import unittest
import datetime
import requests
import time
import json
import os
import sys
#sys.path.append('/home/ddamianidis/workspace/git-repos/tailwind/tw-reporter/src/opt/tw/reporter/bin')
from reportloader.platforms.appnexus.appnexusclient import AppnexusClient
from reportloader.platforms.appnexus.account import Account
class DataPullerTestCase(unittest.TestCase):
    def setUp(self):
        self.appnexus_client = AppnexusClient()
        self.account = Account()
        
    def tearDown(self):
        pass
        
    #@unittest.skip('just skip')       
    def test_1_get_access_token(self):
        self.access_token = self.account.getToken()
        print(self.access_token)
        self.assertNotEqual(self.access_token, False, 
                        'Response data is correct')
    
    #@unittest.skip('just skip')       
    def test_2_get_data(self):
        
        data = self.appnexus_client.read('2018-01-09', '2018-01-10')
        print(data)
        self.assertNotEqual(data, False, 
                        'Response data is correct')    
        
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DataPullerTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)   