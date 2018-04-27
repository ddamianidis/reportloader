import sys
import datetime
import os
import traceback
import functools
import pprint as pp
from reportloader.utils.sshclient import SSH_Client


class platforms(SSH_Client):
            
    def __init__(self):
        super().__init__()
        self.platforms = {}
        # Extract publisher fees table to file
        platforms_file = os.path.join(self.tmp_dir, 'platforms.txt')
        self.do_cmd(self.mysql_prefix + '-e"select * from platforms" >%s' % platforms_file)
        self.get_file(platforms_file, 'platforms.txt')
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
    
    def uncapitalize(self, s):
        if len(s) > 0:
            s = s[0].lower() + s[1:]
        return s
  
        
    def get(self):                                
        # Parse inventory file to get inventory data
        with open('platforms.txt', 'r') as fh:
            for index, line in enumerate(fh):
                row  = line.strip().split('\t')
                if index == 0:
                    fields = row
                    self.stream_logger.debug('Field list %s' % fields)
                    continue
                pl_id = row[fields.index('platform_id')]
                pl_name = row[fields.index('platform_name')]
                pl_name = "smart" if pl_name == "Smartads" else pl_name
                self.platforms[self.uncapitalize(pl_name)] = pl_id


        self.stream_logger.info('Parsed %s platforms entries' % len(self.platforms))
        self.stream_logger.debug('platforms {0}'.format(pp.pformat(self.platforms)))
        
        return self.platforms
    