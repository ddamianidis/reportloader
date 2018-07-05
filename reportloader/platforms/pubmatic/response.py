""" Defines the Response model """

import os
import re
import time
import requests
from reportloader.interface.IResponseClient import IResponseClient
from reportloader.abstractresponse import AbstractResponse

            
class Response(AbstractResponse, IResponseClient):
    """ Class for manage an pubmatic response """
    
    DATE_FIELD =  'date'
    PLACEMENT_NAME_FIELD = 'placement_name'
    ADTAG_ID_FIELD = 'adtag_id'
    SIZE_FIELD = 'size'
    TOTAL_IMPRESSIONS_FIELD = 'total_impressions'       
    RESOLD_IMPRESSIONS_FIELD = 'resold_impressions'
    REVENUE_FIELD = 'revenue'
    REVENUE_DICT_FIELD = 'revenue_dict'
                
    def __init__(self, data_dict: dict):
        self.platform = 'pubmatic'
        fetched_date = str(data_dict[self.DATE_FIELD])
        self.date = str(data_dict[self.DATE_FIELD])
        self.placement_name = str(data_dict[self.PLACEMENT_NAME_FIELD])
        self.adtagid = str(data_dict[self.ADTAG_ID_FIELD])
        self.total_impressions = int(data_dict[self.TOTAL_IMPRESSIONS_FIELD])
        self.resold_impressions = int(data_dict[self.RESOLD_IMPRESSIONS_FIELD])
        self.revenue = float(data_dict[self.REVENUE_FIELD])
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