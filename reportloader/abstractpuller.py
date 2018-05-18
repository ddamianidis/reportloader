import re
import time
import datetime
import functools
from reportloader.utils.currencyconverter import CurrencyConverter

class AbstractPuller():
    
    rates = {}
    
    def date_in_range(self, start, end, thedate, frmt='%Y-%m-%d'):
        st_date = datetime.datetime.strptime(start, frmt).date()
        end_date = datetime.datetime.strptime(end, frmt).date()
        the_date = datetime.datetime.strptime(thedate, frmt).date()
        
        return st_date<=the_date<=end_date
    
    def timeout_ready(timeout=None):
        if not timeout:
            raise Exception('Must provide a timeout to handle')
        def outer_wrap(f):
            @functools.wraps(f)
            def wrapper(self, *args, **kwargs):
                time_sum = 0
                result = False
                while not result:
                    result = f(self, *args, **kwargs)
                    time.sleep(6)
                    time_sum += 3
                    # if timeout riched return
                    if  time_sum > timeout:
                        return False
                return result    
                    
            return wrapper
        return outer_wrap
    
    def get(self):
        """ 
        Sends the request retrieve the report's data.
         
        :returns: returns True if the report is ready otherwise False. 
        """
                 
        placements = self._getData()
        
        if placements is False:
            return False
         
        cc = CurrencyConverter(placements, 'date')    
        self.rates = cc.rates
        for response in placements:
            
            if not response.validate():
                continue
                       
            # replace the '-sp_' with '_'
            response.placement_name = response.placement_name.replace('-sp_', '_')
            
            if self.get_platform()== 'appnexus':
                sizable_placements = []
                underscore_splits = response.placement_name.split('_')
                sizes = underscore_splits[-1].split('-') 
                name_prefix = '_'.join(underscore_splits[:-1])
                response.placement_name = '{0}_{1}_{2}' \
                                        .format(response.placement_id, name_prefix, 
                                                response.size)
            else:                            
                response.placement_id = self.get_id(response.placement_name)
            
            if self.get_platform() in ('taboola', 'smaato', 'facebook'):
                # update the revenue entry and set the eur and usd fields
                revenue_dict = {
                    'EUR' : round(response.revenue * self.rates[response.date], 6),
                    'USD' : round(response.revenue , 6)
                    }
            else:    
                # update the revenue entry and set the eur and usd fields
                revenue_dict = {
                    'EUR' : round(response.revenue, 6),
                    'USD' : round(response.revenue / self.rates[response.date], 6)
                    }                        
            response.revenue_dict = revenue_dict 
         
            yield response
                 
        return 

    def get_id(self, placement_name):
        platform = self.get_platform()
        ID_RX = re.compile(r'^(\d+)_.*$')
        if platform == 'taboola':
            #m_id =  re.match(r'^.*#(\d+)$', placement_name)
            m_id =  re.match(r'^.*-.(\d+)$', placement_name)
            # old naming convention for taboola
            if m_id is None:
                m_id =  re.match(r'^.*#(\d+)$', placement_name)
        elif platform == 'teads':
            m_id =  re.match(r'^.*_(\d+)$', placement_name)        
        else:
            m_id = ID_RX.match(placement_name)
        if m_id is not None:
            return  str(m_id.group(1))
        else:
            return  int(0)
        

class IPuller():
    
    
    def get_platform(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError
    
    def _getData(self):
        """ it's the service to get the access token """
        
        raise NotImplementedError
