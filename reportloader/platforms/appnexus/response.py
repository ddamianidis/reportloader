""" Defines the Response model """

import os
import re
import time
import datetime
import requests
from reportloader.platforms.appnexus.reader import OutputReaderInterface   
from reportloader.platforms.appnexus.report import Report
from reportloader.interface.IResponseClient import IResponseClient   
from reportloader.abstractresponse import AbstractResponse

            
class Response(AbstractResponse, IResponseClient):
    """ Class for manage an appnexus response """
            
    def __init__(self, data_dict: dict):
        m = re.search(r'\d+[xX]\d+', data_dict['size'])
        self.platform = 'appnexus'
        self.date = str(data_dict['day'])
        self.placement_id = str(data_dict['placement_id'])
        self.placement_name = str(data_dict['placement_name'])
        self.buyer_member_id = str(data_dict['buyer_member_id'])
        self.buyer_member_name = str(data_dict['buyer_member_name'])
        self.size = str(m.group(0)) if m is not None else None
        self.total_impressions = int(data_dict['imp_requests'].replace('.0', ''))
        self.resold_impressions = int(data_dict['imps_resold'].replace('.0', ''))
        # define rate after costs
        if self.buyer_member_id == '2026':
            date_30_06_18 = datetime.datetime.strptime('2018-06-30', '%Y-%m-%d').date()
            date_cur = datetime.datetime.strptime(self.date, '%Y-%m-%d').date()
            if date_cur > date_30_06_18:
                rate_after_costs = 0.748
            else:
                rate_after_costs = 0.73
        else:
            rate_after_costs = 1
        self.revenue = round(float(data_dict['revenue'].replace(',', '.')) * rate_after_costs, 
                                   6)        
        self.revenue_dict = data_dict.get('revenue_dict', None)
        self.clicks = int(data_dict['clicks'])
        
        if self.size is None:
            size_part = self.placement_name.rsplit('_', 1)[1]
            m = re.search(r'\d+[xX]\d+', size_part)
            self.size = str(m.group(0)) if m is not None else None
    
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
        if self.size is None:
            return False
            
        return True
        
    