""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime  
from reportloader.platforms.pubmatic.account import Account
from reportloader.platforms.pubmatic.response import Response      
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class PubmaticPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from pubmatic """

    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.access_token = Account().getToken()
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.startdate =  start_date
        self.enddate = end_date
        self.report_id = 0 
        
    def get_platform(self):
        return 'pubmatic' 
        
    def _get_pl_data(self):
        params = {
            'dateUnit': 'date',
            'dimensions': 'date,adTagId',
            'fromDate': self.startdate,
            'toDate': self.enddate,
            'metrics':'revenue,paidImpressions,totalImpressions'
            
        }
    
        headers = {
            'Authorization': 'Bearer {0}'.format(self.access_token),
            'Host': 'api.pubmatic.com'
        }
    
        r = requests.get('http://api.pubmatic.com/v1/analytics/data/publisher/156400', 
                         params=params, headers=headers)
        
        if r.status_code != 200:
            #rootLogger.error('Pabmatic Status code Error {0}'.format(r.status_code))
            return False
        
        SIZE_RX = re.compile(r'^.*_(\d+[xX]\d+)$')
        response_data = r.json()
        #record_transaction('pubmatic', response_data)
        #rootLogger.debug('Pabmatic Revenue summary raw data {0}'.format(json.dumps(response_data, indent=4)))
        fetch_data = {}
        for row in response_data['rows']:
            adtag_id = row[1]
            placement_name = response_data['displayValue']['adTagId'][adtag_id]
            m_size = SIZE_RX.match(placement_name)
            if m_size:
                placement_size = m_size.group(1)
            else:
                placement_size = ''
            entry ={}
            
            entry['date'] = row[0] 
            entry['placement_name'] = placement_name 
            entry['adtag_id'] = adtag_id
            entry['size'] = placement_size
            entry['revenue'] = round(float(row[2]) * 0.85, 6) #substract the comission
            entry['total_impressions'] = int(row[4])
            entry['resold_impressions'] = int(row[3])
            
            key = (entry['placement_name'], entry['date'])
            fetch_data[key] = entry
        
        return fetch_data
    
                            
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        fetch_data = {}
        
        if self.access_token: 
            fetch_data = self._get_pl_data()
        else:
            return False    
                        
        entries = []
        for key, row_dict  in fetch_data.items():
            response = Response(row_dict)
            entries.append(response)
        
        return entries
    