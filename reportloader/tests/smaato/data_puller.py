import unittest
import datetime
import requests
import time
import json
import os
import sys
#sys.path.append('/home/ddamianidis/workspace/git-repos/tailwind/tw-reporter/src/opt/tw/reporter/bin')
#from reportloader.platforms.appnexus.appnexusclient import AppnexusClient
from reportloader.platforms.adsense.account import Account

class pull_smaato_data():
    
    def __init__(self, startdate=None, enddate=None):
        startdate = self._process_date_argument(startdate)
        
        #print('pull_smartadserver_data %s' % date)
    
        # Start pulling the report data from smartadserver
        #rootLogger.info('Start pull_smaato_data')
        
        # validate input arguments
        if startdate is None and enddate is None:
            startdate = days_back_str(1, frmt='%Y-%m-%d')
            enddate = startdate
        elif enddate is None:
            enddate = startdate
            '''startDate = startdate+"T00:00:00"
            enddateObj = datetime.datetime.strptime(startDate, '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(1)
            endDate = enddateObj.strftime('%Y-%m-%dT%H:%M:%S')
            enddate = enddateObj.strftime('%Y-%m-%d')
            '''
        elif startdate is None:    
            raise Exception('startdate is None and enddate is not, this is not acceptable')
        
    #access_token = 'QMZs6M0eyJFoMNr5pao0zYt40psgyp'

    def days_back(self, days):
        return datetime.date.today() -  datetime.timedelta(days)

    def days_back_str(self, days, frmt='%Y-%m-%d'):
        return self.days_back(days).strftime(frmt)

    def yesterday(self):
        return self.days_back(1)
    
    def yesterday_str(self, frmt='%Y-%m-%d'):
        return self.yesterday().strftime(frmt)

    
    def _process_date_argument(self, date):
        if date is None:
            return self.yesterday_str()
        elif date == 'yesterday':
            return self.days_back_str(2)
        else:
            return date


    def get_access_token(self):
        data = {
            "client_id":"gIIvEIJraFHoqMEw4NG3yk81bSX2PrtQejtCyach",
            "client_secret": "pFeEOYa37hKIvdknKZpobKnYMRT87VJYJSe8pWvIIthLJ21YWwLowzzKRFiuLF6FHkLt5SqVKtJD2p5LaIyOJ4nLtUDBZl3oVi8MqjtlDA9ZV8DeLTaApxlkFzHDftqg",
            "grant_type":"client_credentials"    
               
            }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'auth.smaato.com'
        }
    
        r = requests.post('https://auth.smaato.com/v2/auth/token/', data=data, headers=headers)
        print(r.text)
        if r.status_code != 200:
            #rootLogger.error('Error while retrieving access token')
            #rootLogger.error('Status code {0}'.format(r.status_code))
            return False

        ac_data = r.json()
        access_token = ac_data['access_token'] 
        #rootLogger.debug('access_token: {0}'.format(access_token))
        return access_token
    
    def get_total_imprs(self, access_token):
        data = {
            'criteria':{
                "dimension":"Date",
                'child': {
                'dimension':'AdspaceId',
                'fields': [
                    'name'
                    ]
                          }
                },
            'kpi': {
                'incomingAdRequests': True,
                'clicks': True
                },    
            'period':{
                'period_type':'fixed',
                'start_date':startdate,
                'end_date':enddate
        }    
            }

        headers = {
            'Authorization': 'Bearer {0}'.format(access_token),
            'Content-Type': 'application/json',
            'Host': 'api.smaato.com'
        }
    
        r = requests.post('https://api.smaato.com/v1/reporting/', data=json.dumps(data), headers=headers)
        
        if r.status_code != 200:
            #rootLogger.debug('Error json data: {0}'.format(r.text))
            #rootLogger.error('Error while retrieving revenue summary')
            #rootLogger.error('Status code {0}'.format(r.status_code))
            return False
        
        response_data = r.json()
        #record_transaction('smaato', response_data)
        #rootLogger.debug('Revenue summary raw data {0}'.format(json.dumps(response_data, indent=4)))
        
        return response_data
    
    def get_resold_imprs(self, access_token):
        data = {
            'criteria':{
                "dimension":"Date",
                'child': {
                'dimension':'AdspaceId',
                'fields': [
                    'name'
                    ],   
                          'child': {
                               'dimension': 'LineItemType',
                               'child': None
                              }
                          }
                },
            'filters': [
                {
                    'field': 'LineItemType',
                    'values': ['SMX']
                }
                        ],    
            'kpi': {
                'impressions': True,
                'netRevenue': True
                },    
                'period':{
                    'period_type':'fixed',
                    'start_date':startdate,
                    'end_date':enddate
                }    
            }

        headers = {
            'Authorization': 'Bearer {0}'.format(access_token),
            'Content-Type': 'application/json',
            'Host': 'api.smaato.com'
        }
    
        r = requests.post('https://api.smaato.com/v1/reporting/', data=json.dumps(data), headers=headers)
        
        if r.status_code != 200:
            #rootLogger.debug('Error json data: {0}'.format(r.text))
            #rootLogger.error('Error while retrieving revenue summary')
            #rootLogger.error('Status code {0}'.format(r.status_code))
            return False
        
        response_data = r.json()
        #record_transaction('smaato', response_data)
        #rootLogger.debug('Revenue summary raw data {0}'.format(json.dumps(response_data, indent=4)))
        
        return response_data
    
    def get(self):
        ac = self.get_access_token()
        
        if ac != False: 
            total_imprs_res = self.get_total_imprs(ac)
        else:
            return False    
            
        if ac != False: 
            resold_imprs_res = self.get_resold_imprs(ac)
        else:
            return False    
        
        return (total_imprs_res, resold_imprs_res)

class DataPullerTestCase(unittest.TestCase):
    def setUp(self):
        #self.appnexus_client = AppnexusClient()
        self.pull_smaato_data = pull_smaato_data('2018-02-19', '2018-02-20')
        
    def tearDown(self):
        pass
        
    #@unittest.skip('just skip')       
    def test_1_get_creadentials(self):
        self.credentials = self.pull_smaato_data.get_access_token()
        print(self.credentials)
        self.assertNotEqual(self.credentials, False, 
                        'smaato credentials are correct')
    
    @unittest.skip('just skip')       
    def test_2_get_data(self):
        
        data = self.appnexus_client.read('2018-01-09', '2018-01-10')
        print(data)
        self.assertNotEqual(data, False, 
                        'Response data is correct')    
        
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DataPullerTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)   