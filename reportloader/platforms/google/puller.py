""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
from apiclient.discovery import build
from reportloader.utils.currencyconverter import CurrencyConverter        
from reportloader.platforms.google.requestdispatcher import RequestDispatcherInterface
from reportloader.platforms.google.account import AdxAccount
from reportloader.platforms.google.account import AdsenseAccount
from reportloader.platforms.google.requestdispatcher import AdxRequestDispatcher
from reportloader.platforms.google.requestdispatcher import AdsenseRequestDispatcher                    
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller

                    
class GooglePuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """
    
    def __init__(self, startdate, enddate, sub_platform):
        if sub_platform == 'adx':
            self.request_dispatcher = AdxRequestDispatcher(AdxAccount(), startdate, 
                                                    enddate) 
        else:
            self.request_dispatcher = AdsenseRequestDispatcher(AdsenseAccount(), startdate, 
                                                    enddate)
        
    def get_platform(self):
        return 'google'    
    
    def _getData(self):
        placements = self.request_dispatcher.dispatch()
        return placements