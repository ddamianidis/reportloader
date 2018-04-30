""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime
from reportloader.platforms.smart.reader import OutputReaderInterface   
from reportloader.platforms.smart.account import Account
from reportloader.platforms.smart.reader import OutputCsvReader
from reportloader.platforms.smart.response import Response
from reportloader.utils.currencyconverter import CurrencyConverter        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class SmartPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    default_base_url = 'https://reporting.smartadserverapis.com/'
    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.account = Account()
        self.output_reader = OutputCsvReader()
        self.build_url = "https://reporting.smartadserverapis.com/"\
                          + self.account.networkid\
                          +"/reports" 
        self.base_url = self.default_base_url
        self.auth = (self.account.username, self.account.password)
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.startdate =  start_date+"T00:00:00"
        # reporter insterface for end date is closed
        # but appnexus interface for end date is open
        # so we have to add a day in appnexus enddate
        self.enddate = self.date_after_days_str(end_date, 1)
        self.enddate = self.enddate+"T00:00:00"
        self.report_id = 0 
        
    def get_platform(self):
        return 'smart' 
        
    def _download_file(self, url):
        """ 
        Download a file from a given url.
        
        :param: the url of the file. 
        :returns: the body of the report in json format. 
        """
        
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter
        r = requests.get(url, auth=self.auth, stream=True)
        if r.status_code is not 200:
            self.stream_logger.error('Error while downloading teads report')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
                    #f.flush() commented by recommendation from J.F.Sebastian
        return local_filename    
        
    def _build(self):
        """ 
        Sends the request that builds the report.
         
        :returns: returns the report id. 
        """
        
        payload = { }
        rFields = [{"Day":{}},{ "PageName": {} }, { "Auctions": {} }, { "RtbImpressions": {} }, 
                           { "TotalPaidPriceNetworkCurrencyTrueCount": {} }]
        payload["startDate"] = self.startdate
        payload["endDate"] = self.enddate
        payload["fields"] = rFields
        print(json.dumps(payload))
        r = requests.post(self.build_url, auth=self.auth,  
                          data=json.dumps(payload))
        
        if r.status_code is not 201:
            self.stream_logger.error('Error while building smart report')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        
        self.report_id = response_data['taskId']
            
        return 
    
    @AbstractPuller.timeout_ready(500) # timeout in 100 secs    
    def _ready(self):
        """ 
        Sends the request that checks the reports status.
         
        :returns: returns True if the report is ready otherwise False. 
        """
        
        url = '{0}/{1}'.format(self.build_url, self.report_id)                                
        r = requests.get(url, auth=self.auth)

        if r.status_code is not 200:
            self.stream_logger.error('Error in ready smart request')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        self.stream_logger.info('ready status:{0}'.format(response_data["lastTaskInstance"]["instanceStatus"]))        
        if (response_data["lastTaskInstance"]["instanceStatus"] in ("SUCCESS", "EMPTY_REPORT")):
            return True
        else:
            return False    
     
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        self._build()
        ready = self._ready()
        if not ready:
            return False

        download_url = '{0}/{1}/file'.format(self.build_url, self.report_id)    
        csv_file = self._download_file(download_url)
        
        if not csv_file:
            return []

        data = self.output_reader.readOutput(csv_file)
        #data = json.load(open(json_file))
        os.remove(csv_file)
        entries = []
        for pl in data:
            response = Response(pl)
            entries.append(response)
        
        return entries
    
    def date_after_days_str(self, date, days):
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)  
        return d.strftime('%Y-%m-%d')
