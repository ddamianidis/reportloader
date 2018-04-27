""" Defines the report manager of the request """

import json
import datetime
from reportloader.platforms.appnexus.account import Account
from reportloader.platforms.appnexus.account import AccountInterface
        
            
class Report():
    """ Class for building an appnexus report """
    
    default_dimensions = ['day', 'placement_id', 'placement_name', 'size']
    default_metrics = ['day', 'placement_id', 'placement_name', 'size', 
                       'imp_requests', 'imps_resold','clicks', 'revenue']
        
    def __init__(self, startdate, enddate, 
                 rtype='network_analytics', rformat='csv', 
                 dimensions=None,
                 metrics=None):
        self.account = Account()
        self.startdate = startdate
        # reporter insterface for end date is closed
        # but appnexus interface for end date is open
        # so we have to add a day in appnexus enddate
        self.enddate = self.date_after_days_str(enddate, 1)
        self.rtype = rtype
        self.rformat = rformat
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
                        "report":
                        {
                            "format": "%s",
                            "report_type":"%s",
                            "timezone":"UTC",
                            "start_date":"%s",
                            "end_date":"%s",
                            "row_per": %s,
                            "columns": %s,
                            "pivot_report":false,
                            "show_usd_currency":false
                        }    
                    }
                     ''' % (self.rformat, self.rtype, self.startdate, 
                            self.enddate, json.dumps(self.dimensions), 
                            json.dumps(self.metrics), 
                                )
    
    @property
    def headers(self):
        """ 
        Builds and returns the header of the report.
        
        :returns: the header dict of the report. 
        """
        
        return  {            
                    'Authorization': '{0}'.format(self.access_token)
                }
        
    def date_after_days_str(self, date, days):
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)  
        return d.strftime('%Y-%m-%d')