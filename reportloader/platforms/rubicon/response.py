""" Defines the Response model """

import os
import re
import time
import requests
from reportloader.platforms.teads.reader import OutputReaderInterface   
from reportloader.platforms.teads.report import Report
from reportloader.interface.IResponseClient import IResponseClient
from reportloader.abstractresponse import AbstractResponse

            
class Response(AbstractResponse, IResponseClient):
    """ Class for manage an appnexus response """
    
    DATE_FIELD =  'Date'
    PLACEMENT_NAME_FIELD = 'Zone'
    TOTAL_IMPRESSIONS_FIELD = 'Auctions'       
    RESOLD_IMPRESSIONS_FIELD = 'Paid Impressions'
    REVENUE_FIELD_OLD = 'Revenue'
    REVENUE_FIELD_NEW = 'Gross Revenue'
    SIZE_FIELD = 'Size'
    DEAL_FIELD = 'Deal'
    REVENUE_DICT_FIELD = 'revenue_dict'
                
    def __init__(self, data_dict: dict):
        self.platform = 'rubicon'
        #print(data_dict)
        self.date = str(data_dict[self.DATE_FIELD])
        self.placement_name = str(data_dict[self.PLACEMENT_NAME_FIELD])
        self.total_impressions = int(data_dict[self.TOTAL_IMPRESSIONS_FIELD].replace('.0', ''))
        self.resold_impressions = int(data_dict[self.RESOLD_IMPRESSIONS_FIELD].replace('.0', ''))
        if self.REVENUE_FIELD_OLD in data_dict:
            self.revenue = round(float(data_dict[self.REVENUE_FIELD_OLD]) * .84, 6) # deduct 16% commision
        else:
            self.revenue = round(float(data_dict[self.REVENUE_FIELD_NEW]) * .84, 6) # deduct 16% commision    
        self.size = str(data_dict[self.SIZE_FIELD])
        self.deal = str(data_dict[self.DEAL_FIELD])
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
            
        m = re.search(r'\d+[xX]\d+', self.size)
        self.size = m.group(0) if m is not None else None
        # if size is null give it a second chance and  
        # get it from placement name
        if self.size is None:
            size_part = self.placement_name.rsplit('_', 1)[1]
            m = re.search(r'\d+[xX]\d+', size_part)
            self.size = m.group(0) if m is not None else None
        
        if self.size is None:
            return False
                
        return True