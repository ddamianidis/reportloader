""" Module for retrieving appnexus reporting data """
 
from reportloader.interface.IReporterClient import IReporterClient
from reportloader.interface.puller_factory import ReporterPuller
from reportloader.pushtoui.reporterpusher import ReporterPusher


class ReporterClient(IReporterClient):
    """ Class responsible to get appnexus reports """
    
    def __init__(self, platform=None):
        self.platform = platform
        
    def read(self, start_date, end_date):
        """ 
        Get the Network Analytics report () from the appnexus platform. 
        
        :returns: the list of dicts that contains the report's data. 
        """
        
        data_puller_cls = ReporterPuller.factory(self.platform)
        data_puller = data_puller_cls(start_date, end_date, self.platform)
        data = {}
        for pl_sizeless in data_puller.get():
            #print(pl_sizeless)
            key = (pl_sizeless.placement_name, pl_sizeless.date)
            data[key] = pl_sizeless
                
        return data      
    
    def push_to_ui(self, date, mode='prod'):
        
        """ 
        Get the Network Analytics report () from the appnexus platform. 
        
        :returns: the list of dicts that contains the report's data. 
        """
        
        platform = 'all' if not self.platform else self.platform 
        data_pusher_cls = ReporterPusher(platform, date, mode)
        rs = data_pusher_cls.push_dailies_to_ui()
        
        return rs
           
    