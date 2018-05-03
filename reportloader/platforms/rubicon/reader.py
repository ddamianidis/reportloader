""" Module for retrieving appnexus reporting data """
import csv
import re

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
        
        #fh = open(file,'r')
        reader = csv.DictReader(file)
        
        
        fetch_data = {}
        for row in reader:
            m = re.search(r'\d+[xX]\d+', row['Size'])
            #supress decimals if exist
            
            entry = {
                'placement_name': row['Zone'],
                'date': row['Date'],
                'size': m.group(0) if m is not None else None,
                'total_impressions': int(row['Auctions'].replace('.0', '')), #Auctions instead of Impressions 
                'resold_impressions': int(row['Paid Impressions'].replace('.0', '')),
                'revenue': round(float(row['Gross Revenue'] if 'Gross Revenue' in row else row['Revenue']), 6) , # round to match db precision
                'deal': row['Deal'],
            }
            
            key = (entry['placement_name'].lower(), entry['date'])
            if key in fetch_data:
                fetch_data[key]['total_impressions'] += entry['total_impressions']
                fetch_data[key]['resold_impressions'] += entry['resold_impressions']
                fetch_data[key]['revenue'] += entry['revenue']
                #mailerLogger.warn('Found duplicate key {0}. Keeping first entry.'.format(key))
            else:
                fetch_data[key] = entry
                
        fetch_list = [ fetch_data[key] for key in fetch_data ]
                
        if csv  is not None:
            file.close()
        
        return fetch_list         
