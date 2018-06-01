#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
"""

import unittest
import json
import os
import time
import datetime
from reportloader.utils.config import Config
from unittest.mock import MagicMock
from reportloader.tests.TestCase import TestCase
from reportloader.reporterpuller import ReporterPuller
from reportloader.database.mongo import MongoConnector
from reportloader.utils.mongoidtools import ObjectId
from unittest.mock import patch
from jrpc.client import JsonRPCClient


class PullDataTestCase(TestCase):
     
    use_case_valid_commands = [
        'pull_appnexus',
        'pull_adsense',
        'pull_adx',
        'pull_teads',
        'pull_criteo',
        'pull_criteohb',
        'pull_taboola',
        'pull_smart',
        'pull_smaato',
        'pull_facebook',
        'pull_rubicon',
        ]
    
    expected_results = {
        'pull_appnexus':{'deleted_entries_count': 1, 'new_entries_count': 2, 'updated_entries_count': 2, 'identical_entries_count': 2463},
        'pull_adsense':{'identical_entries_count': 17, 'new_entries_count': 2, 'deleted_entries_count': 1, 'updated_entries_count': 2},
        'pull_adx':{'new_entries_count': 2, 'updated_entries_count': 2, 'identical_entries_count': 2677, 'deleted_entries_count': 1},
        'pull_teads':{'updated_entries_count': 2, 'identical_entries_count': 128, 'new_entries_count': 2, 'deleted_entries_count': 1},
        'pull_criteo': {'deleted_entries_count': 1, 'identical_entries_count': 6331, 'new_entries_count': 2, 'updated_entries_count': 2},
        'pull_criteohb':{'deleted_entries_count': 1, 'updated_entries_count': 2, 'identical_entries_count': 42, 'new_entries_count': 2},
        'pull_taboola': {'identical_entries_count': 523, 'deleted_entries_count': 1, 'new_entries_count': 2, 'updated_entries_count': 2},
        'pull_smart':{'deleted_entries_count': 1, 'updated_entries_count': 2, 'new_entries_count': 2, 'identical_entries_count': 418},
        'pull_rubicon':{'deleted_entries_count': 1, 'updated_entries_count': 2, 'new_entries_count': 2, 'identical_entries_count': 2866},
        'pull_smaato': {'deleted_entries_count': 1, 'identical_entries_count': 168, 'updated_entries_count': 2, 'new_entries_count': 2},
        'pull_facebook':{'new_entries_count': 2, 'identical_entries_count': 1, 'updated_entries_count': 2, 'deleted_entries_count': 0}
    }
    
    removed_placements = {
        'pull_appnexus':[
            '4157463_real.gr_politics_970x250',
            '11766412_reader.gr_homepage-1_970x250'
        ],
        'pull_adsense':[
            '12351524_LocaleNetwork_ros_970x250',
            '10011495_alwakeelnews.com_ros_970x250'
        ],
        'pull_adx':[
            '11974846_apkroids.com_ros_300x250', 
            '12686534_babyradio.gr_adblock-1_300x600',
        ],
        'pull_teads':[
            '79096 - inread - meteorologos.gr_11550271',
            '85264 - inRead - themamagers.gr_12350620'
        ],
        'pull_criteo':[
            '12001599_gazzetta.gr_adblock-3_300x250',
            '12001600_gazzetta.gr_adblock-4_300x250'
        ],
        'pull_criteohb':[
            '12552348_merrjep.com_postBid_300x600',
            '12552372_merrjep.al_postBid_300x600'
        ],
        'pull_taboola':[
            'ant1iwo#CY-p11169123',
            'go4it#RO-p10884104'
        ],
        'pull_smart':[
            '7491770_m.businessmagazin.ro_ros-2_300x250',
            '4964992_antenna.gr_ros_300x250'
        ],
        'pull_rubicon':[
            '9017352_go4it.ro_Homepage-branding_970x250',
            '8551373_newsbomb.gr_Autokinito-2_300x250'
        ],
        'pull_smaato':[
            '9905337_alphatv.gr_ros-4_300x250',
            '9905336_alphatv.gr_ros-3_300x250'
        ],
        'pull_facebook':[
            '5600778_m.antena3.ro_allsite_300x250',
            '5665857_m.kanald.ro_allsite_300x250'
        ],
        }
    
    changed_placements = {
        'pull_appnexus':[
            '12480112_frontpages.gr_homepage-ros-side-perf_300x250',
            '11998822_jurnalmm.ro_inarticle_319x49'
        ],
        'pull_adsense':[
            '12531406_savoirville.gr_ros-2_300x250',
            '12484008_LocaleNetwork_ros_970x90'
        ],
        'pull_adx':[
            '12659596_queen.gr_mobile-2_300x250',
            '6601364_lovecooking.gr_ros_728x90'
        ],
        'pull_teads':[
            '78426 - inRead - holdpont.hu_7198944',
            '78427 - inRead - beszeljukmac.com_6045377'
        ],
        'pull_criteo':[
            '4565457_skai.gr_localnews_300x600',
            '10030247_skai.gr_ROS-2_300x250'
        ],
        'pull_criteohb':[
            '12552413_mojtrg.rs_postBid_728x90',
            '12552406_mojtrg.rs_postBid_300x250'
        ],
        'pull_taboola':[
            'webnyeremeny#HU-f11904363',
            'royanews#MENA-p11308540'
        ],
        'pull_smart':[
            '7052351_alphatv.gr_tvshows_970x250',
            '11837266_garbo.ro_ros-2_300x250'
        ],
        'pull_rubicon':[
            '11441382_xe.gr_automoto-ad-details_728x90',
            '9916795_queen.gr_mageirikh-1_728x90'
        ],
        'pull_smaato':[
            '7491935_m.csid.ro_ros-2_300x250',
            '11071848_m.observator.tv_homepage_300x250'
        ],
        'pull_facebook':[
            '3841343_m.alphatv.gr_mobile_300x250',
            '4068005_m.spynews.ro_allsite_300x250'
        ],
        }
    
    request_body = ''
    expected_data = []
    dict_expected_data = {}
    dict_nd_expected_data = {}
    dbdata = []
    command = 'pull_appnexus'
    reports = MongoConnector('reports').collection
    
    @classmethod
    def date_to_mongoid(self, date, daytime=(0,0,0)):
        hours, mins, secs = daytime
        datetimeobj = datetime.datetime.combine(date, datetime.time(hours, mins, secs))
        mongo_id = ObjectId.generate_from_datetime(datetimeobj)
        return mongo_id
    
    @classmethod
    def setCommand(cls, command):
        cls.command = command
        
    @classmethod
    def setInOutData(cls, command):
        cls.platform = cls.getPlatform(command)
        cls.dates = cls.getDates(command)
        cls.expected_data = cls.getExpected(command)
        cls.dict_expected_data = cls.getDictExpected(command)
        cls.dict_nd_expected_data = cls.getDictExpected(command, next_day=True)
        cls.dbdata = cls.getDbData()  
    
    @classmethod
    def getSuiteOfValidTests(cls):
        suite = unittest.TestSuite()
        suite.addTest(cls('test_1_pull_data_insert'))
        suite.addTest(cls('test_2_pull_data_update'))
        suite.addTest(cls('test_3_pull_data_update_revenue'))
        suite.addTest(cls('test_4_pull_data_update_multiple_fields'))
        suite.addTest(cls('test_5_pull_data_insert_different_dates'))
        suite.addTest(cls('test_6_set_pull_mode_mute'))
        suite.addTest(cls('test_7_set_pull_mode_normal'))
        return suite
    
    @classmethod
    def getDbData(cls):
        #selected_placements = cls.selected_placements[command] 
        start_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[0], '%Y-%m-%d')
            )
        end_mongo_id = cls.date_to_mongoid(
                datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d')
                , (23, 0, 0))
        '''
        if cls.platform in ('appnexus', 'adsense', 'smart', 'facebook', 'rubicon'):
            end_mongo_id = cls.date_to_mongoid(
                datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d')
                , (23, 0, 0))
        else:
            end_mongo_id = cls.date_to_mongoid(
                datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d')
                )
        '''        
        # Get db dataset for same date    
        reports_cursor = cls.reports.find(
            {"$and":[{"platform" : cls.platform}, {"_id":{"$gte":start_mongo_id}}, 
                          {"_id":{"$lt":end_mongo_id}}]}, {'_id':0, 'placement_name':0} )
        
        db_entries = []
        for doc in reports_cursor:
            db_entries.append(doc)
        
        return db_entries
          
    @classmethod
    def insertDepricatedDbData(cls):
        platform = cls.getPlatform(cls.command)
        # Get db dataset for same date
        mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime("2018-01-20", '%Y-%m-%d'),
            (12, 0, 0))    
        result = cls.reports.insert({
             "_id": mongo_id,
             "platform" : platform,
             "date" : "2018-01-20",
             "placement_id" : 11974846,
             "placement_name" : "11974846_depricated_apkroids.com_ros_300x250",
             "total_impressions" : 21539,
             "resold_impressions" : 127,
             "revenue" : 0.01,
             "revenue_dict" : {
                 "EUR" : 0.01,
                 "USD" : 0.012221
                 },
            "clicks" : 0
            })
        #print('inserted_count:{0}'.format(result.inserted_count))
        return result
    
    @classmethod
    def removeDepricatedDbData(cls):
        platform = cls.getPlatform(cls.command)
        # Get db dataset for same date    
        result = cls.reports.delete_many(
            {"$and":[{"platform" : cls.platform},  
                     {"placement_name":"11974846_depricated_apkroids.com_ros_300x250"}
                     ]
             })
        return result
          
    @classmethod
    def removeDbData(cls):
        
        start_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[0], '%Y-%m-%d')
            )
        end_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d'), (23,0,0)
            )
        # Get db dataset for same date    
        result = cls.reports.delete_many(
            {"$and":[{"platform" : cls.platform}, {"_id":{"$gte":start_mongo_id}}, 
                          {"_id":{"$lt":end_mongo_id}}]})
        
        return result.deleted_count
    
    @classmethod
    def removeSelectedDbData(cls):
        selected_placements = cls.removed_placements[cls.command]
        start_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[0], '%Y-%m-%d')
            )
        end_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d'), (23,0,0)
            )
        
        # Get db dataset for same date    
        result = cls.reports.delete_many(
            {"$and":[{"platform" : cls.platform}, 
                     {"_id":{"$gte":start_mongo_id}}, 
                     {"_id":{"$lt":end_mongo_id}}, 
                     {"placement_name":{"$in":selected_placements}}
                     ]
             })
        
        return result.deleted_count
    
    @classmethod
    def updateSelectedDbDataRevenueField(cls):
        selected_placements = cls.changed_placements[cls.command]
        start_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[0], '%Y-%m-%d')
            )
        end_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d'), (23,0,0)
            )
           
        result = cls.reports.update_many(
            {"$and":[{"platform" : cls.platform}, 
                     {"_id":{"$gte":start_mongo_id}}, 
                     {"_id":{"$lt":end_mongo_id}},
                     {"placement_name":{"$in":selected_placements}}]},
            {"$set":{"revenue": 100000.0,
             "revenue_dict.EUR":100000.0,
             "revenue_dict.USD":100000.0}})
        
        return result.modified_count
    
    @classmethod
    def updateSelectedDbDataMultipleFields(cls):
        selected_placements = cls.changed_placements[cls.command]
        start_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[0], '%Y-%m-%d')
            )
        end_mongo_id = cls.date_to_mongoid(
            datetime.datetime.strptime(cls.dates[1], '%Y-%m-%d'), (23,0,0)
            )
            
        result = cls.reports.update_many(
            {"$and":[{"platform" : cls.platform}, 
                     {"_id":{"$gte":start_mongo_id}}, 
                     {"_id":{"$lt":end_mongo_id}},
                     {"placement_name":{"$in":selected_placements}}]},
            {"$set":{"revenue": 0.0,
             "revenue_dict.EUR":0.0,
             "revenue_dict.USD":0.0,
             "total_impressions":0,
             "resold_impressions":0
             }})
        
        return result.modified_count
        
    def setUp(self):
        pass
        self.maxDiff = None
        
    def tearDown(self):
        pass
            
    #@unittest.skip('just skip')   
    def test_1_pull_data_insert(self):
        # for teads platform wait before the next request
        if self.platform in ('teads'):
            time.sleep(5)
        #self.maxDiff = None
        # delete data from database
        self.removeDbData()
        
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb'
        
        ret_data = JsonRPCClient("tcp://localhost:5552").rpc_call('pull_data', 
                                platform=self.platform, startdate = self.dates[0],
                                enddate=self.dates[1]
                                )
        
        #ReporterPuller(self.platform, self.dates[0], self.dates[1]).pull_data()                        
        #self.assertEqual(ret_data, True, 'correct pull process')
        self.dbdata = self.getDbData()
        #print(self.dbdata)
        #print(len(self.expected_data))
        self.assertCountEqual(self.dbdata, self.expected_data,
                         'correct pulled data')
                
    #@unittest.skip('just skip')   
    def test_2_pull_data_update(self):
        #if self.platform == 'teads':
        #    time.sleep(5)
        # get expected data 
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = 'localhost'
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()
        self.dbdata = self.getDbData()
        #self.assertEqual(ret_data, True, 
        #                                            'correct pull process')
        # read the data from the database and compare them with 
        # expected data
        self.assertCountEqual(self.dbdata, self.expected_data, 'correct pulled data')
        
    #@unittest.skip('just skip')   
    def test_3_pull_data_update_revenue(self):
        if self.platform in ('teads'):
            time.sleep(5)
            
        # for teads platform wait before the next request
        #self.maxDiff = None
        # delete data from database
        self.updateSelectedDbDataRevenueField()
        self.removeSelectedDbData()
        self.removeDepricatedDbData()
        self.insertDepricatedDbData()
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb'                        
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()

        #print(ret_data)
        expected_results = self.expected_results[self.command]
        #self.assertEqual(ret_data, expected_results, 'results are correct')
        self.dbdata = self.getDbData()
        
        self.assertCountEqual(self.dbdata, self.expected_data,
                         'pulled data are correct')
        
    #@unittest.skip('just skip')   
    def test_4_pull_data_update_multiple_fields(self):
        # for teads platform wait before the next request
        if self.platform in ('teads'):
            time.sleep(5)
        self.maxDiff = None
        # delete data from database
        self.updateSelectedDbDataMultipleFields()
        self.removeSelectedDbData()
        self.removeDepricatedDbData()
        self.insertDepricatedDbData()
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb' 
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()                       
        #print(ret_data)
        expected_results = self.expected_results[self.command]
        #self.assertEqual(ret_data, expected_results, 'results are correct')
        self.dbdata = self.getDbData()
        self.assertCountEqual(self.dbdata, self.expected_data,
                         'correct puremoveDepricatedDbDatalled data')      
        
    #@unittest.skip('just skip')   
    def test_5_pull_data_insert_different_dates(self):
        # for teads platform wait before the next request
        if self.platform in ('teads'):
            time.sleep(5)
        #self.maxDiff = None
        # delete data from database
        self.removeDbData()
        
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb'
        
        # pull data for 2018-01-20
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()
        
        # pull data for next day        
        puller = ReporterPuller(self.platform, self.dates[2], self.dates[3])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_nd_expected_data)                            
        ret_data = puller.pull_data()
        
        # pull data for 2018-01-20                                        
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()
        
        #self.assertEqual(ret_data, True, 'correct pull process')
        self.dbdata = self.getDbData()
        #print(len(self.dbdata))
        self.assertCountEqual(self.dbdata, self.expected_data,
                         'correct pulled data')          
    
    #@unittest.skip('just skip')   
    def test_6_set_pull_mode_mute(self):
        # for teads platform wait before the next request
        if self.platform in ('teads'):
            time.sleep(5)
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb'
        
        JsonRPCClient("tcp://localhost:5552").rpc_call('pull_mode_mute',
                                                                  platform=self.platform)    
        
        ret_data = JsonRPCClient("tcp://localhost:5552").rpc_call('pull_data', 
                                platform=self.platform, startdate = self.dates[0],
                                enddate=self.dates[1]
                                )
        self.assertEqual(ret_data['result'], False, 'correct pulled data')
        
    #@unittest.skip('just skip')   
    def test_7_set_pull_mode_normal(self):
        # for teads platform wait before the next request
        if self.platform in ('teads'):
            time.sleep(5)
        # set env variable for mongo host
        #os.environ['MONGO_HOST'] = '127.0.0.1'
        #os.environ['MONGO_HOST'] = 'mongodb'
        
        JsonRPCClient("tcp://localhost:5552").rpc_call('pull_mode_normal',
                                                                  platform=self.platform)    
        
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()
                                
        self.assertNotEqual(ret_data, False, 'correct pulled data')
        
        
        JsonRPCClient("tcp://localhost:5552").rpc_call('pull_mode_normal',
                                                                  platform=self.platform)    
        
        puller = ReporterPuller(self.platform, self.dates[0], self.dates[1])
        
        puller.reporter_client.read = MagicMock(
                                            return_value=self.dict_expected_data)                            
        ret_data = puller.pull_data()
        
        self.assertNotEqual(ret_data, False, 'correct pulled data')    
        

if __name__ == '__main__':
    
    # Run valid use cases
    for cmd in PullDataTestCase.use_case_valid_commands:
        print('###############################################')
        print('Running TestSuite for use case: {0}'.format(cmd))
        print('###############################################')
        PullDataTestCase.setCommand(cmd)
        PullDataTestCase.setInOutData(cmd)
        #suite = unittest.TestLoader().loadTestsFromTestCase(ReporterAPITestCase)
        suite = PullDataTestCase.getSuiteOfValidTests()
        tr = unittest.TextTestRunner(verbosity=2).run(suite)
        #tr = unittest.TestCase.defaultTestResult()
        #print('fail list: {0}'.format(tr.failures))
        if tr.failures:
            exit(1)
        
