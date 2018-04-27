""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
from apiclient.discovery import build
from reportloader.platforms.criteo.response import Response
from reportloader.utils.config import Config        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger
                    
class CriteoPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    default_build_url='http://api.appnexus.com/report'
    default_base_url='https://api.appnexus.com'
    
    def __init__(self, startdate, enddate, sub_platform):
        self.token = self.token = Config.getParam(sub_platform, 'apitoken')
        self.sub_platform = sub_platform
        self.startdate = startdate
        self.enddate = enddate
        self.stream_logger = StreamLogger.getLogger(__name__)    
    
    def get_platform(self):
        return 'criteo'    
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        url = "https://publishers.criteo.com/api/2.0/stats.json"
        params = {
            'apitoken' : self.token,
            'begindate': self.startdate,
            'enddate': self.enddate,
            'metrics': 'PlacementName;Date;TotalImpression;TakeRate;Revenue;Click',
        }
    
        r = requests.get(url=url, params=params)
    
        try:
            data = r.json()
        except simplejson.scanner.JSONDecodeError:
            self.stream_logger.error('Did not get valid json response.'.format(r.text))
            self.stream_logger.error('response text is {0}'.format(r.text))
            return False
        
        res_list = []
        for data_row in data:
            m = re.match(r'^(\d{4}-\d{2}-\d{2})T00:00:00$', data_row['date'])
            if m is None:
                self.stream_logger.error('Could not parse placement date {0}'.format(data_row['date']))
                return False
            
            date_str = m.group(1)
            # Check revenue is in euro
            if data_row['revenue']['currency'] != 'EUR':
                self.stream_logger.error('Currency is not euro {0}'.format(data_row['revenue']['currency']))
                return False
            
            row_dict = {
                'platform':self.sub_platform,
                Response.DATE_FIELD: date_str,
                Response.PLACEMENT_NAME_FIELD: data_row['placementName'],
                Response.TOTAL_IMPRESSIONS_FIELD: int(data_row['totalImpression']),
                Response.RESOLD_IMPRESSIONS_FIELD: int(float(data_row['takeRate']) * int(data_row['totalImpression'])),
                Response.REVENUE_FIELD: round(float(data_row['revenue']['value']), 6),
                Response.CLICKS_FIELD:int(data_row['click']),
            }
            res_list.append(Response(row_dict))    
        return res_list