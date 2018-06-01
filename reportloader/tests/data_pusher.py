import unittest
import datetime
import requests
import time
import json
import os
import sys
#sys.path.append('/home/ddamianidis/workspace/git-repos/tailwind/tw-reporter/src/opt/tw/reporter/bin')
from reportloader.reporterclient import ReporterClient
from reportloader.database.mongo import MongoConnector
from jrpc.client import JsonRPCClient


class DataPusherTestCase(unittest.TestCase):
    
    
    def setUp(self):
        self.reporter_client = ReporterClient()
        self.reports = MongoConnector('reports').collection
        
    def tearDown(self):
        pass
    
    def updateReports(self):
        
        # update teads placement name   
        self.reports.update_many(
            {"$and":[{"platform" : "teads"}, 
                     {"placement_name":"77422 - inRead -  sportfm.gr_5687603"}]},
            {"$set":{"placement_name": "77422 - inRead -  sportfm.gr5687603"}})
        
        # update adx size   
        self.reports.update_many(
            {"$and":[{"platform" : "adx"}, 
                     {"placement_name":"11974846_apkroids.com_ros_300x250"}]},
            {"$set":{"placement_name": "11974846_apkroids.com_ros300x250"}})
        
        # update taboola size field in placement name   
        self.reports.update_many(
            {"$and":[{"platform" : "taboola"}, 
                     {"placement_name":"insider#300x250#GR-r11409755"}]},
            {"$set":{"placement_name": "insider#300250#GR-r11409755"}})
        
        # update taboola revenue   
        self.reports.update_many(
            {"$and":[{"platform" : "criteo"}, 
                     {"placement_name":"11777559_ma7room.com_ros_970x250"}]},
            {"$set":{"revenue_dict.USD": "10,34"}})
        
        # update criteo revenue   
        self.reports.update_many(
            {"$and":[{"platform" : "criteo"}, 
                     {"placement_name":"12001599_gazzetta.gr_adblock-3_300x250"}]},
            {"$set":{"revenue_dict.EUR": "10,35"}})
        
        return True
        
    #@unittest.skip('just skip')       
    def test_1_push_all(self):
        self.maxDiff = None
        expected = {'found': 8285, 'inserted': 8267, 'inserted_eurohoops': 528, 'rejected_id': 8, 'rejected_size': 2, 'rejected_inventory': 6, 'rejected_pub_fee': 0, 'rejected_revenue': 2, 'messages': 
                    ['Platform adsense. Rejecting "TheFrog_300x250". Could_not_parse_placement_id', 
                    'Platform adsense. Rejecting "Test_MSN_Remnant_300x250". Could_not_parse_placement_id', 
                    'Platform adsense. Rejecting "Test_MSN_Remnant_728x90". Could_not_parse_placement_id', 
                    'Platform adsense. Rejecting "Houselife Adsense 300x250". Could_not_parse_placement_id',
                    'Platform adsense. Rejecting "YL_300x250". Could_not_parse_placement_id', 
                    'Platform adx. Rejecting "11974846_apkroids.com_ros300x250". Could_not_parse_placement_size', 
                    'Platform adx. Rejecting "(No tag)". Could_not_parse_placement_id', 
                    'Platform teads. Rejecting "77422 - inRead -  sportfm.gr5687603". Could_not_parse_placement_id', 
                    'Platform criteo. Rejecting "12001599_gazzetta.gr_adblock-3_300x250". revenue_is_not_float', 
                    'Platform criteo. Rejecting "12341051_missbloom.bg_homepage-ros-b-perf_300x600". Could_not_find_it_in_inventory_table', 
                    'Platform criteo. Rejecting "12341048_missbloom.bg_homepage-ros-b-perf_300x250". Could_not_find_it_in_inventory_table', 
                    'Platform criteo. Rejecting "11777559_ma7room.com_ros_970x250". revenue_is_not_float', 
                    'Platform criteo. Rejecting "12341055_missbloom.bg_homepage-ros-perf_970x250". Could_not_find_it_in_inventory_table', 
                    'Platform taboola. Rejecting "mother#GR-p9964478_not_used - DEACTIVATED". Could_not_parse_placement_id', 
                    'Platform taboola. Rejecting "insider#300250#GR-r11409755". Could_not_parse_placement_size', 
                    'Platform rubicon. Rejecting "12341050_missbloom.bg_homepage-ros-a-perf_300x600". Could_not_find_it_in_inventory_table', 
                    'Platform rubicon. Rejecting "12341056_missbloom.bg_homepage-ros-perf_728x90". Could_not_find_it_in_inventory_table', 
                    'Platform rubicon. Rejecting "12341055_missbloom.bg_homepage-ros-perf_970x250". Could_not_find_it_in_inventory_table']}
        
        self.updateReports()
        
        #rs = self.reporter_client.push_to_ui('2018-01-20', 'test')
        ret_data = JsonRPCClient("tcp://localhost:5552").rpc_call('push_data', platform='all',
                                            date = '2018-01-20', mode='test')
        print(ret_data['result'])
        
         # order messages in both dicts
        if 'messages' in expected:
             expected['messages'] = sorted(expected['messages'])
        
        if 'messages' in ret_data['result']:
             ret_data['result']['messages'] = sorted(ret_data['result']['messages'])
        
        self.assertDictEqual(ret_data['result'], expected,  'Push to UI is correct')
        #self.assertNotEqual(ret_data['result'], False,  'Push to UI is correct')
    
    #@unittest.skip('just skip')       
    def test_2_push_adsense(self):
        self.maxDiff = None
        expected = {
            'found': 21, 
            'inserted': 16, 
            'inserted_eurohoops': 1, 
            'rejected_id': 5, 
            'rejected_size': 0, 
            'rejected_inventory': 0, 
            'rejected_pub_fee': 0, 
            'rejected_revenue': 0, 
            'messages': ['Platform adsense. Rejecting "Houselife Adsense 300x250". Could_not_parse_placement_id', 
                         'Platform adsense. Rejecting "Test_MSN_Remnant_300x250". Could_not_parse_placement_id', 
                         'Platform adsense. Rejecting "Test_MSN_Remnant_728x90". Could_not_parse_placement_id', 
                         'Platform adsense. Rejecting "TheFrog_300x250". Could_not_parse_placement_id', 
                         'Platform adsense. Rejecting "YL_300x250". Could_not_parse_placement_id']
                    }
        
        self.updateReports()
        
        #rs = self.reporter_client.push_to_ui('2018-01-20', 'test')
        ret_data = JsonRPCClient("tcp://localhost:5552").rpc_call('push_data', platform='adsense',
                                            date = '2018-01-20', mode='test')
        print(ret_data['result'])
        
        self.assertDictEqual(ret_data['result'], expected,  'Push to UI is correct')
        #self.assertNotEqual(ret_data['result'], False,  'Push to UI is correct')    
        
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(DataPusherTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)   