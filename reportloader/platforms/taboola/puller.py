""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
from apiclient.discovery import build
from reportloader.platforms.taboola.response import Response
from reportloader.platforms.taboola.account import Account        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class TaboolaPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    default_build_url='http://api.appnexus.com/report'
    default_base_url='https://api.appnexus.com'
    
    def __init__(self, startdate, enddate, sub_platform=None):
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.account = Account()
        self.account_id = self.account.account_id
        self.token = self.account.getToken()
        self.startdate = startdate
        self.enddate = enddate    
    
    def get_platform(self):
        return 'taboola'    
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        auth_headers = {
        'Authorization': 'Bearer {0}'.format(self.token)
        }
        
        # Get allowed accounts to parse
        r = requests.get('https://backstage.taboola.com/backstage/api/1.0/users/current/allowed-accounts', headers=auth_headers)
        if r.status_code != 200:
            self.stream_logger.error('Invalid taboola accounts, status:{0}'.format(r.status_code))
            return False
    
        response_data = r.json()
    
        # Parse response data
        placement_names = []
        for entry in response_data['results']:
            if entry['type'] == 'PARTNER' and 'PUBLISHER' in entry['partner_types']:
                placement_names.append(entry['name'])
        
        # Get revenue summary
        r = requests.get('https://backstage.taboola.com/backstage/api/1.0/{0}/reports/revenue-summary/'
                         'dimensions/day_site_placement_breakdown?start_date={1}&end_date={2}'\
                         .format(self.account_id, self.startdate, self.enddate), headers=auth_headers)
    
        if r.status_code != 200:
            self.stream_logger.error('Invalid taboola report, status:{0}'.format(r.status_code))
            return False
    
        response_data = r.json()
    
        # Process data
        res_list = []
        db_insert_entries = {}
        currency_not_usd_count = 0
        invalid_date_count = 0
        duplicate_keys = 0
        for entry in response_data['results']:
            # Skip excess entries
            if entry['publisher_name'] not in placement_names:
                continue
    
            # Skip erroneous currency entries
            if entry['currency'] != 'USD':
                currency_not_usd_count += 1
                continue
            only_date = entry['date'].split()[0]
            # Skip erroneous date entries
            if not self.date_in_range(self.startdate, self.enddate, only_date):
            #if date not in entry['date']:
                invalid_date_count += 1
                continue
    
            db_insert_key = ( entry['publisher_name'], only_date )
            db_insert_entry = {
                Response.PLACEMENT_NAME_FIELD: entry['publisher_name'],
                Response.DATE_FIELD: only_date,
                Response.PAGE_VIEWS_WITH_ADS_FIELD: entry['page_views_with_ads'],
                Response.PAGE_VIEWS_FIELD: entry['page_views'],
                Response.REVENUE_FIELD: entry['ad_revenue'],
                Response.GROSS_REVENUE_FIELD: entry['revenue'] if entry['revenue'] not in [None, ""] else 0,
                Response.CLICKS_FIELD: entry['clicks'],
            }
            if db_insert_key in db_insert_entries:
                duplicate_keys += 1
                continue
            res_list.append(Response(db_insert_entry))
        return res_list    
