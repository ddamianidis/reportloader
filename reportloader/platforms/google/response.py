""" Defines the Response model """

import os
import re
import time
import requests
from reportloader.platforms.appnexus.reader import OutputReaderInterface   
from reportloader.platforms.appnexus.report import Report
from reportloader.interface.IResponseClient import IResponseClient   
from reportloader.abstractresponse import AbstractResponse
    

class AdsenseResponse(AbstractResponse, IResponseClient):
    
    DATE_FIELD =  'date'
    PLACEMENT_ID_FIELD = 'adsense_id'
    PLACEMENT_NAME_FIELD = 'adsense_name'
    SIZE_FIELD = 'size'
    TOTAL_IMPRESSIONS_FIELD = 'total_impressions'       
    RESOLD_IMPRESSIONS_FIELD = 'resold_impressions'
    REVENUE_FIELD = 'revenue'
    REVENUE_DICT_FIELD = 'revenue_dict'
    CLICKS_FIELD = 'clicks'
    
    def __init__(self, data_dict: dict):
        self.platform = 'adsense'
        self.date = data_dict[self.DATE_FIELD]
        self.placement_id = data_dict[self.PLACEMENT_ID_FIELD]
        self.placement_name = data_dict[self.PLACEMENT_NAME_FIELD]
        self.size = data_dict[self.SIZE_FIELD]
        self.total_impressions = data_dict[self.TOTAL_IMPRESSIONS_FIELD]
        self.resold_impressions = data_dict[self.RESOLD_IMPRESSIONS_FIELD]
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
        
        # if size is still None skip
        #if self.size is None:
        #    return False
            
        return True
    

class AdxResponse(AbstractResponse, IResponseClient):
    
    DATE_FIELD =  'date'
    PUBLISHER_ID_FIELD = 'publisher_id'
    PLACEMENT_ID_FIELD = 'adx_placement_id'
    PLACEMENT_NAME_FIELD = 'placement_name'
    TOTAL_IMPRESSIONS_FIELD = 'total_impressions'       
    RESOLD_IMPRESSIONS_FIELD = 'resold_impressions'
    REVENUE_FIELD = 'revenue'
    REVENUE_DICT_FIELD = 'revenue_dict'
    CLICKS_FIELD = 'clicks'
    
    def __init__(self, data_dict: dict):
        self.platform = 'adx'
        self.date = data_dict[self.DATE_FIELD]
        self.publisher_id = data_dict[self.PUBLISHER_ID_FIELD]
        self.placement_id = str(data_dict[self.PLACEMENT_ID_FIELD])
        self.placement_name = data_dict[self.PLACEMENT_NAME_FIELD]
        self.total_impressions = data_dict[self.TOTAL_IMPRESSIONS_FIELD]
        self.resold_impressions = data_dict[self.RESOLD_IMPRESSIONS_FIELD]
        self.revenue = data_dict[self.REVENUE_FIELD]
        self.revenue_dict = data_dict.get(self.REVENUE_DICT_FIELD, None)
        self.clicks = data_dict[self.CLICKS_FIELD]
        self.placement_id = data_dict.get('placement_id', 0)
        
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
        
        # if size is still None skip
        #if self.size is None:
        #    return False
            
        return True
    