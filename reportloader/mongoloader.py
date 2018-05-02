import MySQLdb
import MySQLdb.cursors
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import json
import datetime
import simplejson
import sys
import os
import re
from reportloader.utils.mongoidtools import ObjectId
from reportloader.utils.config import Config
from reportloader.database.mongo import MongoConnector
 
class mongoLoader():
    
    map_table = {
        'adsense':'adsense_revenue_daily',
        'adx':'adx_revenue_daily',
        'rubicon': 'rubicon_revenue_daily',
        #'appnexus': 'appnexus_revenue_daily',
        'criteo': 'criteo_revenue_daily',
        'criteohb': 'criteohb_revenue_daily',
        'fb': 'fb_revenue_daily',
        'smart': 'smartads_revenue_daily',
        'smaato': 'smaato_revenue_daily',
        'taboola': 'taboola_revenue_daily',
        'teads': 'teads_revenue_daily',
    }
    
    def __init__(self):
        self.mysql_db = self._connect_mysql()
        self.data = []
    
    def _connect_mysql(self):
        self.mysql_db = MySQLdb.connect(
                host = Config.getParam('mysql', 'host'),
                user = Config.getParam('mysql', 'user'),
                passwd = Config.getParam('mysql', 'password'),
                db = Config.getParam('mysql', 'db'),
                cursorclass=MySQLdb.cursors.DictCursor)
        return self.mysql_db
        
    def _set_id_from_datetime(self, datetimestr):
        dt = datetime.datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S')
        return ObjectId.generate_from_datetime(dt)
    
    def get_id(self, placement_name, platform):
        
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
            return  int(m_id.group(1))
        else:
            return  int(0)
                    
    def _get_mysql_data(self, platform):
        pl_data = []
        cursor = self.mysql_db.cursor()
        cursor.execute('SELECT * FROM {0} '.format(self.map_table[platform]))    
        for row in cursor.fetchall():
            #print('{0}'.format(row))
            row['platform'] = platform
            datetimeobj = datetime.datetime.combine(row['date'], datetime.time.min)
            #row['date'] = datetimeobj 
            row['date'] = str(row['date'])
            row['_id'] = ObjectId.generate_from_datetime(datetimeobj)
            if platform == 'adsense':
                row['placement_name'] = row['adsense_name']
            
            if platform == 'taboola':
                row['placement_name'] = row['site']
                    
            row['placement_id'] = str(self.get_id(row['placement_name'], platform))
            revenue_dict = {
                'EUR' : round(row['revenue'], 6) if row['revenue'] else 0,
                'USD' : round(row['revenue_usd'] , 6) if row['revenue_usd'] else 0
                }
            row['revenue_dict'] = revenue_dict 
            pl_data.append(row)
            
        return pl_data    

    def load(self, platform=None):
        reports = MongoConnector('reports').collection
        filter_dict = {'platform':platform} if platform else {}
        reports.delete_many(filter_dict)
        pl_table = [platform] if platform else self.map_table 
        for pl in pl_table:
            mdata = self._get_mysql_data(pl)
            #print(pl)
            try:
                reports.insert_many(mdata)
            except BulkWriteError as bws:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                