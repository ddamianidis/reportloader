""" Module for retrieving appnexus reporting data """

class IReporterClient(object):
    """ Interface class for accessing platform clients """
           
    def read(self, start_date, end_date):
        """  
        """
                        
        raise NotImplementedError         
    