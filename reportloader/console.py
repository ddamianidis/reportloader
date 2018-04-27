from pymongo import MongoClient
from pymongo import InsertOne, DeleteMany, ReplaceOne, UpdateOne
from pymongo.errors import BulkWriteError
import argparse
import datetime
import copy
from reportloader.mongoloader import mongoLoader
from reportloader.utils.mongoidtools import ObjectId
from reportloader.reporterpuller import ReporterPuller

parser = argparse.ArgumentParser(description='report loader commands')
parser.add_argument('--startdate', default=None)
parser.add_argument('--enddate', default=None)
parser.add_argument('--platform', default=None)
args = parser.parse_args()

def datetime_now():
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M:%S')

def days_back(days):
    return datetime.date.today() -  datetime.timedelta(days)

def days_back_str(days, frmt='%Y-%m-%d'):
    return days_back(days).strftime(frmt)

def yesterday():
    return days_back(1)

def yesterday_str(frmt='%Y-%m-%d'):
    return yesterday().strftime(frmt)

def today_str(frmt='%Y-%m-%d'):
    return datetime.date.today().strftime(frmt)

def date_in_range(start, end, thedate, frmt='%Y-%m-%d'):
    st_date = datetime.datetime.strptime(start, frmt).date()
    end_date = datetime.datetime.strptime(end, frmt).date()
    the_date = datetime.datetime.strptime(thedate, frmt).date()
    
    return st_date<=the_date<=end_date 

def _process_date_argument(date):
    if date is None:
        return yesterday_str()
    elif date == 'yesterday':
        return days_back_str(2)
    else:
        return date
        
def mysql_to_mongo():
    mongoLoader().load(args.platform)

def date_to_mongoid(date):
    datetimeobj = datetime.datetime.combine(date, datetime.time.min)
    mongo_id = ObjectId.generate_from_datetime(datetimeobj)
    return mongo_id     

def pull_appnexus_data():
    startdate=args.startdate 
    enddate=args.enddate
    
    startdate = _process_date_argument(startdate)

    # validate input arguments
    if startdate is None and enddate is None:
        startdate = days_back_str(1, frmt='%Y-%m-%d')
        enddate = today_str()
    elif enddate is None:
        enddate = today_str()
    elif startdate is None:    
        raise Exception('startdate is None and enddate is not,' 
                        'this is not acceptable')
    
    reporter_puller = ReporterPuller('appnexus', startdate, enddate)
            
    return reporter_puller.pull_data()