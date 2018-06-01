from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
import datetime
import copy
import sys
import json
from reportloader.reporterclient import ReporterClient
from reportloader.utils.mongoidtools import ObjectId
from reportloader.database.mongo import MongoConnector
from reportloader.utils.logger import StreamLogger
from reportloader.pullmode import PullModeKeeper

class ReporterPuller():
        
    def __init__(self, platform, startdate, enddate):
        self.startdate = startdate     
        self.enddate = enddate
        self.platform = platform
        self.reports = MongoConnector('reports').collection
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.reporter_client = ReporterClient(self.platform)
                
    def date_to_mongoid(self, date, daytime=(12,0,0)):
        hours, mins, secs = daytime
        datetimeobj = datetime.datetime.combine(date, datetime.time(hours, mins, secs))
        mongo_id = ObjectId.generate_from_datetime(datetimeobj)
        return mongo_id     
    
    def pull_data(self):
        
        '''
        def jdefault(o):
            return o.__dict__   
        '''
        if PullModeKeeper.isMuted(self.platform):
            return False
            
        db_insert_entries = self.reporter_client.read(self.startdate, self.enddate)
        
        # just to produce the data to mock the above function
        '''
        l = {str(k):v for k,v in db_insert_entries.items()}
        print(json.dumps(l, default=jdefault))
        #print(db_insert_entries)
        exit()
        '''
        # if db_insert_entries is empty means
        # an error occured 
        if not db_insert_entries:
            return False
        
        start_mongo_id = self.date_to_mongoid(datetime.datetime.strptime(self.startdate, '%Y-%m-%d'),
                                              (0, 0, 0))
        end_mongo_id = self.date_to_mongoid(datetime.datetime.strptime(self.enddate, '%Y-%m-%d'),
                                            (23, 0, 0))
        # Get db dataset for same date
        reports_cursor = self.reports.find(
            {"$and":
             [
                 {"platform" : self.platform}, 
                 {"_id":{"$gte":start_mongo_id}}, 
                 {"_id":{"$lte":end_mongo_id}}
            ]
             }
                                           )
        db_entries = {}
        for doc in reports_cursor:
            key = ( doc['placement_name'], doc['date'] )
            db_entries[key] = doc
        #self.stream_logger.info('len of db entries:{0}'.format(len(db_entries)))    
        # Insert rows into database
        entries_to_insert = []
        new_entries_count = 0
        updated_entries_count = 0
        identical_entries_count = 0
        deleted_entries_count = 0
        
        bulk_update_requests = []
        #self.stream_logger.info('db_insert_entries:{0}'.format(db_insert_entries))
        # update the existing entries
        for key, entry in db_insert_entries.items():
            # add custom mongo id
            if key not in db_entries:
                entry['_id'] = self.date_to_mongoid(datetime.datetime
                                                           .strptime(entry['date'], 
                                                                     '%Y-%m-%d'))
                new_entries_count += 1
                entries_to_insert.append(entry if type(entry) is dict else entry.dict())
            else:
                del db_entries[key]['_id']
                #print('from platform:{0}, from db:{1}'.format(type(entry), type(db_entries[key])))
                match_dict = entry if type(entry) is dict else entry.dict()
                if db_entries[key] == match_dict:
                    #self.stream_logger.info('For identical db entry:{0}'.format(entry.dict()))
                    identical_entries_count += 1
                else:
                    #self.stream_logger.info('For update db entry:{0}'.format(entry.dict()))
                    updated_entries_count += 1
                    # update the entry in database
                    replace_filter = {'platform': self.platform, 
                                      'placement_name':key[0],
                                      'date':key[1]
                                     }
                    bulk_update_requests.append(ReplaceOne(replace_filter, 
                                                           entry if type(entry) is dict else entry.dict()))
                    #reports_col.replace_one(replace_filter, entry)
                del db_entries[key]
        
        # update existing documents
        try:
            if bulk_update_requests:
                self.reports.bulk_write(bulk_update_requests)
        except BulkWriteError as bwe:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            print('BulkWriteError: {0}, {1}, {2}'.format(exc_type, 
                                                                   exc_value, 
                                                                   exc_traceback))
            return False#pprint(bwe.details)        
        # insert new entries
        if entries_to_insert:
            self.reports.insert_many(entries_to_insert)
        
        # delete the entries that are no more exist in the platform's statistics        
        deleted_entries_count = len(db_entries)        
        for key in db_entries:
            delete_filter = {'platform': self.platform, 
                                      'placement_name':key[0],
                                      'date':key[1]
                            }
            self.reports.delete_one(delete_filter)
        results = {
            "new_entries_count": new_entries_count,
        "updated_entries_count": updated_entries_count,
        "identical_entries_count": identical_entries_count,
        "deleted_entries_count": deleted_entries_count
        }    
        #self.stream_logger.info('Results:{0}'.format(results))
        return results