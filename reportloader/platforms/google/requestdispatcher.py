""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import datetime
from apiclient.discovery import build
from reportloader.platforms.google.response import AdsenseResponse
from reportloader.platforms.google.response import AdxResponse
from reportloader.utils.currencyconverter import CurrencyConverter    
from reportloader.utils.logger import StreamLogger    
      
class RequestDispatcherInterface():
    """The 'AccountInterface' class declares the interface that must be
    implemented by all account.
    """

    def dispatch(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError                    
                    
class AdsenseRequestDispatcher(RequestDispatcherInterface):
    """ Class responsible for retrieving reporting data from appnexus """
    
    def __init__(self, account, startdate, enddate):
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.http = account.authenticate()
        self.account_id = account.account_id
        self.startdate =startdate
        # reporter insterface for end date is closed
        # but appnexus interface for end date is open
        # so we have to add a day in appnexus enddate
        self.enddate = self.date_after_days_str(enddate, 1) 
                        
    def dispatch(self):
        """ 
        
        """
        
        service = build('adsense', 'v1.4', http=self.http)
        
        if not service:
            self.stream_logger.error('Adsense error service')
            return False
        
        # request report
        result = service.accounts().reports().generate(
                    accountId=self.account_id,
                    startDate=self.startdate, endDate=self.enddate,
                    useTimezoneReporting=True,
                    metric=[
                        'AD_REQUESTS',
                        'MATCHED_AD_REQUESTS',
                        'CLICKS',
                        'EARNINGS',
                    ],
                    dimension=[
                        'AD_UNIT_ID',
                        'AD_UNIT_NAME',
                        'AD_UNIT_SIZE_NAME',
                        'DATE',
                    ],
                ).execute()

        if not result:
            self.stream_logger.error('Adsense error result')
            return False

        # Check currency is euro
        for item in result['headers']:
            if item['name'] == 'EARNINGS' and item['currency'] != 'EUR':
                return False    
                
        # Setup translation dict    
        translate = { header['name'] : index for index, header in enumerate(result['headers'])}
        res_list = []
        for data_row in result['rows']:
            row_dict = {
                AdsenseResponse.DATE_FIELD: result['startDate'],
                AdsenseResponse.PLACEMENT_ID_FIELD: data_row[translate['AD_UNIT_ID']],
                AdsenseResponse.PLACEMENT_NAME_FIELD: data_row[translate['AD_UNIT_NAME']],
                AdsenseResponse.SIZE_FIELD: data_row[translate['AD_UNIT_SIZE_NAME']],
                AdsenseResponse.TOTAL_IMPRESSIONS_FIELD: int(data_row[translate['AD_REQUESTS']]),
                AdsenseResponse.RESOLD_IMPRESSIONS_FIELD: int(data_row[translate['MATCHED_AD_REQUESTS']]),
                AdsenseResponse.REVENUE_FIELD: round(float(data_row[translate['EARNINGS']]), 6),
                AdsenseResponse.CLICKS_FIELD:int(data_row[translate['CLICKS']]),
            }
            res_list.append(AdsenseResponse(row_dict))    
            
        return res_list
    
    def date_after_days_str(self, date, days):
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)  
        return d.strftime('%Y-%m-%d')
    
class AdxRequestDispatcher(RequestDispatcherInterface):
    """ Class responsible for retrieving reporting data from appnexus """
    
    def __init__(self, account, startdate, enddate):
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.http = account.authenticate()
        self.account_id = account.account_id
        self.startdate =startdate
        self.enddate =enddate    
                        
    def dispatch(self):
        """ 
        
        """
        
        service = build('adexchangeseller', 'v2.0', http=self.http)
        
        if not service:
            self.stream_logger.error('Adx error service')
            return False

        result = service.accounts().reports().generate(
                    accountId='pub-2500372977609723',
                    startDate=self.startdate, endDate=self.enddate,
                    metric=[
                        'AD_REQUESTS',
                        'MATCHED_AD_REQUESTS',
                        'CLICKS',
                        'EARNINGS',
                    ],
                    dimension=[
                        'AD_CLIENT_ID',
                        'AD_TAG_CODE',
                        'AD_TAG_NAME',
                        'DATE'
                    ],
                ).execute()
        
        if not result:
            self.stream_logger.error('Adx error result')
            return False
        
        # Check currency is euro
        for item in result['headers']:
            if item['name'] == 'EARNINGS' and item['currency'] != 'EUR':
                return False    
                
        # Setup translation dict    
        translate = { header['name'] : index for index, header in enumerate(result['headers'])}
        
        if result['totalMatchedRows'] == '0':
            result['rows'] = []
            
        res_list = []
        for data_row in result['rows']:
            row_dict = {
                AdxResponse.DATE_FIELD: data_row[translate['DATE']],
                AdxResponse.PUBLISHER_ID_FIELD: data_row[translate['AD_CLIENT_ID']],
                AdxResponse.PLACEMENT_ID_FIELD: data_row[translate['AD_TAG_CODE']],
                AdxResponse.PLACEMENT_NAME_FIELD: data_row[translate['AD_TAG_NAME']],
                AdxResponse.TOTAL_IMPRESSIONS_FIELD: int(data_row[translate['AD_REQUESTS']]),
                AdxResponse.RESOLD_IMPRESSIONS_FIELD: int(data_row[translate['MATCHED_AD_REQUESTS']]),
                AdxResponse.REVENUE_FIELD: round(float(data_row[translate['EARNINGS']]), 6),
                AdxResponse.CLICKS_FIELD:int(data_row[translate['CLICKS']]),
            }
            res_list.append(AdxResponse(row_dict))    
            
        return res_list