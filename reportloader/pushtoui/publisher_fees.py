import sys
import datetime
import os
import traceback
import functools
from reportloader.utils.sshclient import SSH_Client

class publisher_fees(SSH_Client):
            
    def __init__(self):
        super().__init__()
        self.publisher_fees = {}
        # Extract publisher fees table to file
        publisher_fees_file = os.path.join(self.tmp_dir, 'publisher_fees.txt')
        self.do_cmd(self.mysql_prefix + '-e"select * from publishers_fees" >%s' % publisher_fees_file)
        self.get_file(publisher_fees_file, 'publisher_fees.txt')
        self.get()
    
    def jsonmethod(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            try:
                result = f(self, *args,**kwargs)
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                # Log exception message and stacktrace
    
                self.stream_logger.error('Error encoutered %s' % exc_value)
                for line in traceback.format_tb(exc_traceback):
                    self.stream_logger.error(line.strip())
                result = False
            return result
        return wrapper    
        
    def get(self):                                
        # Parse publisher fees file
        now = datetime.datetime.now()
        with open('publisher_fees.txt', 'r') as fh:
            for index, line in enumerate(fh):
                row  = line.strip().split('\t')
                if index == 0:
                    fields = row
                    self.stream_logger.debug('Field list %s' % fields)
                    continue
                publisher_id = row[fields.index('publisherid')]
                fees = row[fields.index('fees')]
                fees_effective_from = row[fields.index('fees_effective_from')]
                fees_when = datetime.datetime.strptime(fees_effective_from, '%Y-%m-%d %H:%M:%S')
    
                # Check if fees date is in the past
                now_delta = now - fees_when
                if now_delta.days < 0:
                    continue
    
                if publisher_id not in self.publisher_fees:
                    self.publisher_fees[publisher_id] = (float(fees), fees_when)
                else:
                    fees_delta = fees_when - self.publisher_fees[publisher_id][1]
                    if fees_delta.days >= 0:
                        self.publisher_fees[publisher_id] = (float(fees), fees_when)
        
        return self.publisher_fees 