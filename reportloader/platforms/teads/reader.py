""" Module for retrieving appnexus reporting data """
import json


class OutputReaderInterface():
    """The 'OutputReaderInterface' class declares the interface that must be
    implemented by all output_readers.
    """

    def readOutput(self):
        raise NotImplementedError

class OutputJsonReader(OutputReaderInterface):
    """ Class responsible to read csv outputs """
    
    def __init__(self):
        pass
        
    def readOutput(self, file):
        """ 
        Read the contents of the csv file and returns the data in a
        list of dicts. 
        
        :returns: the list of dicts that contains the report's data. 
        """
        fetch_data = json.load(open(file))
        
        return fetch_data         
    