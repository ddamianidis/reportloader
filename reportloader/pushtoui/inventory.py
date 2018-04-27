import sys
import datetime
import os
import traceback
import functools
import pprint as pp
from reportloader.utils.sshclient import SSH_Client


class inventory(SSH_Client):
            
    def __init__(self):
        super().__init__()
        self.inventory = {}
        # Extract publisher fees table to file
        inventory_file = os.path.join(self.tmp_dir, 'inventory.txt')
        self.do_cmd(self.mysql_prefix + '-e"select * from inventory" >%s' % inventory_file)
        self.get_file(inventory_file, 'inventory.txt')
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
        # Parse inventory file to get inventory data
        with open('inventory.txt', 'r') as fh:
            for index, line in enumerate(fh):
                row  = line.strip().split('\t')
                if index == 0:
                    fields = row
                    self.stream_logger.debug('Field list %s' % fields)
                else:
                    pl_id = row[fields.index('placement_id')]
                    pub_id = row[fields.index('publisher_id')]
                    site_id = row[fields.index('site_id')]
                    entry = {
                        'placement_id': pl_id,
                        'publisher_id': pub_id,
                        'site_id': site_id,
                        'publisher_name': row[fields.index('publisher_name')],
                        'placement_name': row[fields.index('placement_name')],
                    }
                    self.stream_logger.debug('Adding inventory entry %s' % entry)
                    self.inventory[pl_id] = entry

        self.stream_logger.info('Parsed %s inventory entries' % len(self.inventory))
        self.stream_logger.debug('inventory_dict {0}'.format(pp.pformat(self.inventory)))
        
        return self.inventory
    