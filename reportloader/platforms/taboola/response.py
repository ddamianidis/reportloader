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
    PAGE_VIEWS_FIELD = 'page_views'       
    PAGE_VIEWS_WITH_ADS_FIELD = 'page_views_with_ads'
    RESOLD_IMPRESSIONS_FIELD = 'resold_impressions'
    REVENUE_FIELD = 'revenue'
    GROSS_REVENUE_FIELD = 'gross_revenue'
    REVENUE_DICT_FIELD = 'revenue_dict'
    CLICKS_FIELD = 'clicks'
    
    def __init__(self, data_dict: dict):
        self.platform = 'taboola'
        self.date = data_dict[self.DATE_FIELD]
        self.placement_name = data_dict[self.PLACEMENT_NAME_FIELD]
        self.page_views = data_dict[self.PAGE_VIEWS_FIELD]
        self.page_views_with_ads = data_dict[self.PAGE_VIEWS_WITH_ADS_FIELD]
        self.revenue = data_dict[self.REVENUE_FIELD]
        self.revenue_dict = data_dict.get(self.REVENUE_DICT_FIELD, None)
        self.clicks = data_dict[self.CLICKS_FIELD]
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