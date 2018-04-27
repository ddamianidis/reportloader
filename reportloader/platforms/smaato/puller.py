""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime  
from reportloader.platforms.smaato.account import Account
from reportloader.platforms.smaato.response import Response      
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class SmaatoPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """

    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.access_token = Account().getToken()
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.startdate =  start_date
        self.enddate = end_date
        self.report_id = 0 
        
    def get_platform(self):
        return 'smaato' 
        
    def get_total_imprs(self):
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
                'start_date':self.startdate,
                'end_date':self.enddate
        }    
            }

        headers = {
            'Authorization': 'Bearer {0}'.format(self.access_token),
            'Content-Type': 'application/json',
            'Host': 'api.smaato.com'
        }
    
        r = requests.post('https://api.smaato.com/v1/reporting/', data=json.dumps(data), headers=headers)
        
        if r.status_code != 200:
            self.stream_logger.debug('Error json data: {0}'.format(r.text))
            self.stream_logger.error('Error while retrieving revenue summary')
            self.stream_logger.error('Status code {0}'.format(r.status_code))
            return False
        
        response_data = r.json()
        self.stream_logger.debug('Revenue summary raw data {0}'.format(json.dumps(response_data, indent=4)))
        
        return response_data
    
    def get_resold_imprs(self):
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
                    'start_date':self.startdate,
                    'end_date':self.enddate
                }    
            }

        headers = {
            'Authorization': 'Bearer {0}'.format(self.access_token),
            'Content-Type': 'application/json',
            'Host': 'api.smaato.com'
        }
    
        r = requests.post('https://api.smaato.com/v1/reporting/', data=json.dumps(data), headers=headers)
        
        if r.status_code != 200:
            self.stream_logger.debug('Error json data: {0}'.format(r.text))
            self.stream_logger.error('Error while retrieving revenue summary')
            self.stream_logger.error('Status code {0}'.format(r.status_code))
            return False
        
        response_data = r.json()
        
        self.stream_logger.debug('Revenue summary raw data {0}'.format(json.dumps(response_data, indent=4)))
        
        return response_data
     
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        if self.access_token: 
            total_imprs_res = self.get_total_imprs()
        else:
            return False    
            
        if self.access_token != False: 
            resold_imprs_res = self.get_resold_imprs()
        else:
            return False    
            
        fetch_data = {}
        
        for row in total_imprs_res:
            placement_name = row['criteria'][1]['meta']['name']
            adspace_id = row['criteria'][1]['value']
            SIZE_RX = re.compile(r'^.*_(\d+[xX]\d+)$')
            m_size = SIZE_RX.match(placement_name)
            placement_size = m_size.group(1)
            entry ={}
            fetched_date_array = row['criteria'][0]['value']
            fetched_date = '{0}-{1}-{2}'.format(fetched_date_array[0], 
                                                '0{0}'.format(fetched_date_array[1]) if len(str(fetched_date_array[1])) == 1 else fetched_date_array[1], 
                                                '0{0}'.format(fetched_date_array[2]) if len(str(fetched_date_array[2])) == 1 else fetched_date_array[2])
            #m = re.search(r'\d+x\d+', row['Size'])
            entry['date'] = fetched_date 
            entry['placement_name'] = placement_name 
            entry['adspace_id'] = adspace_id
            entry['size'] = placement_size
            entry['revenue_usd'] = float(0)
            entry['total_impressions'] = row['kpi']['incomingAdRequests']
            entry['resold_impressions'] = 0
            entry['clicks'] = int(row['kpi']['clicks'])
            #entry['revenue'] = str(float(row['RtbImpressions']) *  .88 * float(row['RtbEcpm']) / 1000)
            key = (entry['placement_name'], entry['date'])
            fetch_data[key] = entry
             
        for row in resold_imprs_res:
            fetched_date_array = row['criteria'][0]['value']
            fetched_date = '{0}-{1}-{2}'.format(fetched_date_array[0], 
                                                '0{0}'.format(fetched_date_array[1]) if len(str(fetched_date_array[1])) == 1 else fetched_date_array[1], 
                                                '0{0}'.format(fetched_date_array[2]) if len(str(fetched_date_array[2])) == 1 else fetched_date_array[2])
            placement_name = row['criteria'][1]['meta']['name']
            key = (placement_name, fetched_date)
            if fetch_data[key]:
                fetch_data[key].update({'resold_impressions':int(row['kpi']['impressions']),
                                        'revenue_usd':float(row['kpi']['netRevenue'])})
                
        entries = []
        for key, row_dict  in fetch_data.items():
            response = Response(row_dict)
            entries.append(response)
        
        return entries
    