import unittest
import json
import os

class TestCase(unittest.TestCase):
    
    @classmethod   
    def readFixture(cls, command, format_type='json'):
        cfolder = os.path.dirname(__file__)
        with open('{0}/{1}/{2}.{3}'.format(cfolder, 'data', 
                                               command, format_type)) as f:
            content = f.read()
        return content
    
    @classmethod
    def getExpected(cls, command, format_type='json'):
        content = cls.readFixture('{0}_expected'.format(command), format_type)
        if format_type == 'json':
            res = json.loads(content)
        else:
            res = content
        return  res
    
    @classmethod
    def getDates(cls, command):
        
        if command == 'pull_appnexus':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit open
        elif command == 'pull_adsense':
            res = ('2018-01-20', '2018-01-20', '2018-04-21', '2018-04-21')#end limit open
        elif command == 'pull_adx':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed
        elif command == 'pull_teads':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed
        elif command == 'pull_criteo':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed
        elif command == 'pull_criteohb':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed            
        elif command == 'pull_taboola':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed
        elif command == 'pull_smart':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit open
        elif command == 'pull_smaato':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit closed
        elif command == 'pull_facebook':
            res = ('2016-12-05', '2016-12-05', '2016-12-06', '2016-12-06')#end limit open
        elif command == 'pull_rubicon':
            res = ('2018-01-20', '2018-01-20', '2018-01-21', '2018-01-21')#end limit open     
        else:
            res = content
        return  res    

    @classmethod
    def getPlatform(cls, command):
        
        if command == 'pull_appnexus':
            res = 'appnexus'
        elif command == 'pull_adsense':
            res = 'adsense'
        elif command == 'pull_adx':
            res = 'adx'
        elif command == 'pull_teads':
            res = 'teads'            
        elif command == 'pull_criteo':
            res = 'criteo'
        elif command == 'pull_criteohb':
            res = 'criteohb'
        elif command == 'pull_taboola':
            res = 'taboola'
        elif command == 'pull_smart':
            res = 'smart'
        elif command == 'pull_smaato':
            res = 'smaato'
        elif command == 'pull_facebook':
            res = 'facebook'      
        elif command == 'pull_rubicon':
            res = 'rubicon'                      
        else:
            res = content
        return  res