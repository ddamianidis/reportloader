""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime
import imaplib
import email
import io
import csv
import base64
from apiclient import errors
from reportloader.platforms.rubicon.reader import OutputReaderInterface   
from reportloader.platforms.rubicon.account import Account
from reportloader.platforms.rubicon.reader import OutputCsvReader
from reportloader.platforms.rubicon.response import Response
from reportloader.utils.currencyconverter import CurrencyConverter        
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class RubiconPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
            
    def __init__(self, start_date, end_date, sub_platform=None):
        self.account = Account()
        # Login to gmail
        self.mail_connection = self.account.mail_connection        
        self.output_reader = OutputCsvReader()
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.startdate =  start_date
        # reporter insterface for end date is closed
        # but appnexus interface for end date is open
        # so we have to add a day in appnexus enddate
        self.enddate = self.date_after_days_str(end_date, 1)
        
    def get_platform(self):
        return 'rubicon' 

    def getMailAfterDays(self, date, days, get_first=True):        
        # search for date+2
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)
        d1 = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days+1) 
             
        search_str = 'from:reports-delivery@rubiconproject.com after:{0} before:{1}' \
        .format(d.strftime('%Y-%m-%d'), d1.strftime('%Y-%m-%d'))     
        
        data = self.mail_connection.users().messages().list(userId='me',
                                                            labelIds=['INBOX'], q=search_str).execute()
        
        if 'messages' not in data:
            raise Exception('Could not search mailbox')

        # Fetch last message ids
        len_of_messages = data['resultSizeEstimate']        
        if len_of_messages > 0:
            # fetch the first message which contains the 
            # 2 days before data
            if get_first:
                idx = 0 
            else:
                idx = len_of_messages - 1
                # report doesn't exist yet
                if idx == 0:
                    return False
            message_id = data['messages'][idx]['id']    
            message = self.mail_connection.users().messages().get(userId='me',
                                                             id=message_id, format='raw').execute()    
            
            if 'raw' not in message:
                return False
            
            msg_str = base64.urlsafe_b64decode(message['raw'].encode('utf-8'))
            mime_msg = email.message_from_bytes(msg_str)
            
            return mime_msg
            
        else:
            return False
            
            
    def getGmailAttachement(self, date, sender, decode=True):
    
        # get the mail message after 2 days
        msg = self.getMailAfterDays(date, 1)
        
        # if no mail exists get the mail message after 1 day
        if not msg:
            msg = self.getMailAfterDays(date, 0, False)
        
        # if no mail exists no report exists    
        if not msg:
            return False
        
         
        # Get mail date
        date_str = msg['Date']
        
        # Parse the useful time data
        m = re.search(r'\d{2} \w{3} \d{4} \d{2}:\d{2}:\d{2} [+-]\d{4}', date_str)
        if m is None:
            time_diff = 'Undefined'
        else:
            mail_when_utc = datetime.datetime.strptime(m.group(0), '%d %b %Y %H:%M:%S %z')
            utc_now = datetime.datetime.now(datetime.timezone.utc)
            utc_delta = utc_now - mail_when_utc
            time_diff = ' {0} days {1}:{2} hours'.format(utc_delta.days, utc_delta.seconds // 3600, (utc_delta.seconds % 3600) // 60)
        
        parts = msg.get_payload()
        
        
        if decode == True:
            csv_attachement = parts[-1].get_payload(decode=True).decode("utf-8")
        else:
            csv_attachement = parts[-1].get_payload()
            
        return (csv_attachement, date_str, time_diff) 
     
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        sd = datetime.datetime.strptime(self.startdate, '%Y-%m-%d').date()
        ed = datetime.datetime.strptime(self.enddate, '%Y-%m-%d').date()
        cd = sd
        entries = []
        while cd < ed:
            cd_str = cd.strftime('%Y-%m-%d')
            
            csv_attachement, date_str, time_diff = self.getGmailAttachement(cd_str,
                                                           'reports-delivery@rubiconproject.com', 
                                                           False)
            
            csv_file = io.StringIO(csv_attachement)
            if not csv_file:
                continue
            
            data = self.output_reader.readOutput(csv_file)
            
            for pl in data:
                response = Response(pl)
                entries.append(response)
            cd = cd + datetime.timedelta(1)
           
        return entries
    
    def date_after_days_str(self, date, days):
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)  
        return d.strftime('%Y-%m-%d')
