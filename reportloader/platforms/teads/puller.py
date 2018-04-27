""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime
from reportloader.platforms.teads.reader import OutputReaderInterface   
from reportloader.platforms.teads.report import Report
from reportloader.platforms.teads.reader import OutputJsonReader
from reportloader.platforms.teads.response import Response
from reportloader.utils.currencyconverter import CurrencyConverter        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class TeadsPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    default_build_url='https://api.teads.tv/v1/analytics/custom'
    default_base_url = 'https://api.teads.tv/'
    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.report = Report(start_date, end_date)
        self.output_reader = OutputJsonReader()
        self.build_url = self.default_build_url 
        self.base_url = self.default_base_url
        self.report_id = 0
        self.access_token = 0
        self.rates = {}
        self.stream_logger = StreamLogger.getLogger(__name__)
    
    def get_platform(self):
        return 'teads' 
        
    def _download_file(self, url):
        """ 
        Download a file from a given url.
        
        :param: the url of the file. 
        :returns: the body of the report in json format. 
        """
        
        local_filename = url.split('/')[-1]
        # NOTE the stream=True parameter
        r = requests.get(url, stream=True)
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
        #print(self.report.body)
        r = requests.post(self.build_url, headers=self.report.headers, 
                        data=self.report.body)
        
        if r.status_code is not 200:
            self.stream_logger.error('Error while building teads report')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        
        self.report_id = response_data['id']
            
        return True
    
    @AbstractPuller.timeout_ready(100) # timeout in 100 secs
    def _ready(self):
        """ 
        Sends the request that checks the reports status.
         
        :returns: returns True if the report is ready otherwise False. 
        """
        
        time.sleep(5)
        url = 'https://api.teads.tv/v1/analytics/custom/{0}'.format(self.report_id)                                
        r = requests.get(url,
                          headers=self.report.headers)

        if r.status_code is not 200:
            self.stream_logger.error('Error in ready teads request')
            self.stream_logger.error('Status code {0}'\
                                     .format(r.status_code))
            return False

        response_data = r.json()
        #print(response_data)
        if (response_data['status'] == "finished"  
              and response_data['valid'] == True):
            self.download_url = response_data['uri']
            self.stream_logger.debug('Teads report url:{0}'.format(self.download_url))
            return True
        else:
            return False    
                        
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """

        built = self._build()
        #while not self._ready():
        #    time.sleep(1)
        if not built:
            return False  
        ready = self._ready()
        if not ready:
            return False
        json_file = self._download_file(self.download_url)
        
        if not json_file:
            return []

        data = self.output_reader.readOutput(json_file)
        #data = json.load(open(json_file))
        os.remove(json_file)
        entries = []
        invalid_date_count = 0
        for entry in data['lines']:        
            only_date = entry[1].replace("/", "-")
            revenue = entry[4].split('\\')[0]
            
            # Skip erroneous date entries
            if not self.date_in_range(self.report.startdate, self.report.enddate, only_date):
            #if date not in entry['date']:
                invalid_date_count += 1
                continue
    
            insert_entry = {
                'placement_name': entry[0],
                'date': only_date,
                'total_impressions': entry[2].replace(' ', ''), # remove blanks
                'resold_impressions': entry[3].replace(' ', ''), # remove blanks
                'revenue': revenue.replace(' ', '').replace('N/A', 
                                                            '0'), # remove blanks,
            }
            response = Response(insert_entry)
            entries.append(response)
        
        return entries