""" Module for retrieving appnexus reporting data """
import csv

class OutputReaderInterface():
    """The 'OutputReaderInterface' class declares the interface that must be
    implemented by all output_readers.
    """

    def readOutput(self):
        raise NotImplementedError

class OutputCsvReader(OutputReaderInterface):
    """ Class responsible to read csv outputs """
    
    def __init__(self):
        pass
        
    def readOutput(self, file):
        """ 
        Read the contents of the csv file and returns the data in a
        list of dicts. 
        
        :returns: the list of dicts that contains the report's data. 
        """
        
        fh = open(file,'r')
        reader = csv.DictReader(fh, delimiter=';')
        fetch_data = []
        for row in reader:
            fetch_data.append(row)
                
        if csv  is not None:
            fh.close()
        
        return fetch_data         
