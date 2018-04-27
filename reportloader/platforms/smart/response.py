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
    
    DATE_FIELD =  'Day'
    PLACEMENT_NAME_FIELD = 'PageName'
    TOTAL_IMPRESSIONS_FIELD = 'Auctions'       
    RESOLD_IMPRESSIONS_FIELD = 'RtbImpressions'
    REVENUE_FIELD = 'TotalPaidPriceNetworkCurrencyTrueCount'
    REVENUE_DICT_FIELD = 'revenue_dict'
                
    def __init__(self, data_dict: dict):
        self.platform = 'smart'
        print(data_dict)
        fetched_date = str(data_dict[self.DATE_FIELD])
        self.date = fetched_date[:-9] # remove the tail returned from date, 2017-01-26T00:00:00 => 2017-01-26
        self.placement_name = str(data_dict[self.PLACEMENT_NAME_FIELD])
        self.total_impressions = int(data_dict[self.TOTAL_IMPRESSIONS_FIELD].replace('.0', ''))
        self.resold_impressions = int(data_dict[self.RESOLD_IMPRESSIONS_FIELD].replace('.0', ''))
        self.revenue = round(float(data_dict[self.REVENUE_FIELD]) * .88, 6) # deduct 12% commision
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