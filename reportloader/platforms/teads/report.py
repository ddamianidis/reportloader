""" Defines the report manager of the request """

import json
from reportloader.platforms.teads.account import AccountInterface
from reportloader.platforms.teads.account import Account        
            
class Report():
    """ Class for building an appnexus report """
    
    default_dimensions = ['placement', 'day']
    default_metrics = ['impression', 'publisher_billable_volume', 
                       'teads_billing']
        
    def __init__(self, startdate, enddate,  
                 dimensions=None,
                 metrics=None):
        self.account = Account()
        self.startdate = startdate
        self.enddate = enddate
        self.dimensions = self.default_dimensions if dimensions is None else dimensions 
        self.metrics = self.default_metrics if metrics is None else metrics
        self.access_token = self.account.getToken()
            
    @property
    def body(self):
        """ 
        Builds and returns the body of the report.
        
        :returns: the body of the report in json format. 
        """
        
        return '''
                    {                
                         "dimensions": %s,
                         "metrics": %s,
                         "filters": {
                             "ads": [],
                            "adsources": [],
                            "advertisers": [],
                            "ad_sources": [],
                            "ad_status": [],
                            "browser": [],
                            "country": [],
                            "connection_buy_type": [],
                            "creatives": [],
                            "device": [],
                            "formats": [],
                            "insertions": [],
                            "insertion_status": [],
                            "operating_system": [],
                            "packs": [],
                            "page": [],
                            "placements": [],
                            "placement_status": [],
                            "publishers": [],
                            "scenarios": [],
                            "websites": [],
                            "date": {
                                  "start": "%sT00:00:00+01:00",
                    
                                  "end": "%sT23:59:00+01:00",
                    
                                  "timezone": "Europe/Athens"
                            }
                         },
                         "format": "jsonv1"
                    
                    }
                 ''' % (json.dumps(self.dimensions), json.dumps(self.metrics), 
                        self.startdate, self.enddate)
    
    @property
    def headers(self):
        """ 
        Builds and returns the header of the report.
        
        :returns: the header dict of the report. 
        """
        
        return  {            
                    'Content-Type':'application/json',
                    'Accept':'application/json',
                    'Authorization': 'Bearer {0}'.format(self.access_token)
                }
        