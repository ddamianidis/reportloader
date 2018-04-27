""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
from reportloader.platforms.appnexus.reader import OutputReaderInterface   
from reportloader.platforms.appnexus.report import Report
from reportloader.platforms.appnexus.response import Response
from reportloader.platforms.appnexus.reader import OutputCsvReader
from reportloader.utils.currencyconverter import CurrencyConverter        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger
                    
class AppnexusPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    default_build_url='http://api.appnexus.com/report'
    default_base_url='https://api.appnexus.com'
    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.report = Report(start_date, end_date)
        self.output_reader = OutputCsvReader()
        self.build_url = self.default_build_url  
        self.base_url = self.default_base_url 
        self.report_id = 0
        self.access_token = 0
        self.stream_logger = StreamLogger.getLogger(__name__)
    
    def get_platform(self):
        return 'appnexus'
        
    def _download_file(self, url, headers):
        """ 
        Download a file from a given url.
        
        :param: the url of the file. 
        :returns: the body of the report in json format. 
        """
        
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter
        r = requests.get(url, headers=headers, stream=True)
        
        if r.status_code is not 200:
            self.stream_logger.error('Error while downloading appnexus report')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False
        
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
        
        r = requests.post(self.build_url, headers=self.report.headers, 
                        data=self.report.body)

        if r.status_code is not 200:
            self.stream_logger.error('Error while building appnexus report')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        #print(response_data)
        if response_data['response']['status'] != 'OK':
            self.stream_logger.error('Error while building appnexus report')
            self.stream_logger.error('Status code {0}'\
                                     .format(response_data['response']['status']))
            
        self.report_id = response_data['response']['report_id']    
        return 
    
    @AbstractPuller.timeout_ready(100) # timeout in 100 secs
    def _ready(self):
        """ 
        Sends the request that checks the reports status.
         
        :returns: returns True if the report is ready otherwise False. 
        """
                   
        r = requests.get('https://api.appnexus.com/report?id=%s'
                          % (self.report_id), 
                          headers=self.report.headers)
        
        if r.status_code is not 200:
            self.stream_logger.error('Error in ready appnexus request')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        
        if response_data['response']['execution_status'] == 'ready':
            self.download_url = response_data['response']['report']['url']
            self.stream_logger.debug('Appnexus report url:{0}'.format(self.download_url))
            return True
        else:
            return False    
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        self._build()
        #while not self._ready():
        #    time.sleep(1)
        ready = self._ready()
        if not ready:
            return False    
        csv_file = self._download_file('%s/%s'
                                       % (self.base_url, self.download_url),
                                       self.report.headers)
        if not csv_file:
            return []
         
        data = self.output_reader.readOutput(csv_file)
        
        os.remove(csv_file)
        entries = []
        for pl in data:
            response = Response(pl)
            entries.append(response)
        
        return entries
    