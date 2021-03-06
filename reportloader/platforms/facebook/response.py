""" Defines the Response model """

import os
import re
import time
import requests   
from reportloader.interface.IResponseClient import IResponseClient
from reportloader.abstractresponse import AbstractResponse

            
class Response(AbstractResponse, IResponseClient):
    """ Class for manage an appnexus response """
    
    DATE_FIELD =  'date'
    PLACEMENT_NAME_FIELD = 'placement_name'
    IMPRESSIONS_FIELD = 'impressions'
    AD_REQUESTS_FIELD = 'ad_requests'
    AD_REQUESTS_RESOLD_FIELD = 'ad_requests_resold'       
    CLICKS_FIELD = 'clicks'
    REVENUE_FIELD = 'revenue_usd'
    REVENUE_DICT_FIELD = 'revenue_dict'
                
    def __init__(self, data_dict: dict):
        self.platform = 'facebook'
        self.date = str(data_dict[self.DATE_FIELD])
        self.placement_name = str(data_dict[self.PLACEMENT_NAME_FIELD])
        self.total_impressions = int(data_dict[self.AD_REQUESTS_FIELD])
        self.resold_impressions = int(data_dict[self.AD_REQUESTS_RESOLD_FIELD])
        self.revenue = round(float(data_dict[self.REVENUE_FIELD]), 6)
        self.revenue_dict = data_dict.get('revenue_dict', None)
        self.placement_id = str(data_dict.get('placement_id', 0))
                
    def validate(self):    
        """ 
        Validate the response according to placement_name and size fields.
        
        :returns: True in success otherwise False. 
        """
        #ignore row if any value except revenue_usd is None
        #is_any_null(entry.values())
        # ignore row if the placement name is 0
        if self.placement_name == '0':
            return False        
            
        return True