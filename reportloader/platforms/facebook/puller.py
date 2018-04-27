""" Module for retrieving appnexus reporting data """

import os
import re
import time
import requests
import json
import datetime  
import pprint as pp
from reportloader.platforms.facebook.account import Account
from reportloader.platforms.facebook.response import Response      
from reportloader.abstractpuller import AbstractPuller
from reportloader.abstractpuller import IPuller
from reportloader.utils.logger import StreamLogger

                    
class FacebookPuller(AbstractPuller, IPuller):
    """ Class responsible for retrieving reporting data from appnexus """

    app_list = [
        'facebook-gr', 'facebook-ro', 'facebook-hu', 
        #'facebook-mena'
    ]
    
    placement_dict = {}
    
    id_to_name_dict = {
       '806773609464021': '3841343_m.alphatv.gr_mobile_300x250',
       '1396721050347010':'4068005_m.spynews.ro_allsite_300x250',
       '1412366688782446':'4896771_m.gsp.ro_allsite_300x250',
       '1801720546761463':'5576503_m.contactcars.com_Mobile_300x250',
       '1812477432352441':'5576505_m.filfan.com_Mobile_300x250', 
       '1396727447013037':'5600778_m.antena3.ro_allsite_300x250',
       '1396729903679458':'5665857_m.kanald.ro_allsite-sp_300x250',
       '1801717290095122':'9730311_m.huffpostarabi.com_ros_320x50',
       '1801714076762110':'9756176_m.addiyar.com_ros_320x50',
       '1812477622352422':'9875856_m.filfan_homepage-mpu-1_300x250',
       '1812477672352417':'9875857_m.filfan_homepage-mpu-2_300x250',
       '1812477715685746':'9875858_m.filfan_ros-article-mpu_300x250',
       '1812477749019076':'9875859_m.filfan_ros-mpu_300x250',
        '1812477792352405':'9875860_m.filfan_ros-footer-mpu_300x250',
        '1812477895685728':'9875861_m.filfan_homepage-footer-mpu_300x250',
        '1812477935685724':'9875862_m.filfan_ros-gallery-mpu_300x250',
    }
        
    def __init__(self, start_date, end_date, sub_platform=None):
        #self.rootLogger.info('pb init called')
        self.date = start_date
        self.stream_logger = StreamLogger.getLogger(__name__)
        self.startdate =  self.date_to_epoch(start_date)
        # reporter insterface for end date is closed
        # but appnexus interface for end date is open
        # so we have to add a day in appnexus enddate
        self.enddate = self.date_after_days_str(end_date, 1)
        self.enddate = self.date_to_epoch(self.enddate)
        self.access_tokens = {app:Account(app).getToken() for app in self.app_list}
        self.account_ids = {app:Account(app).id for app in self.app_list}
        self.account_names = {app:Account(app).name for app in self.app_list}

    def date_to_epoch(self, date):
        new_date = datetime.datetime.strptime(date, '%Y-%m-%d')  + datetime.timedelta(hours=12)
        t_format = '%Y-%m-%d %H:%M'
        date_epoch = int(time.mktime(time.strptime(new_date.strftime(t_format), t_format)))
        return date_epoch
       
    def get_platform(self):
        return 'facebook' 
        
    def get_revenue(self, app):
        params = {
            'since': self.startdate,
            'until': self.enddate,
            'summary': 'true',
            'event_name': 'fb_ad_network_revenue',
            'aggregateBy': 'SUM',
            'breakdowns[0]': 'placement',
            'access_token': self.access_tokens[app],
        }
        url = 'https://graph.facebook.com/v2.8/{0}/app_insights/app_event/'.format(self.account_ids[app])
        
        r = requests.get(url, params=params)
        if r.status_code != 200:
            print('Status code was {0}'.format(r.status_code))
            self.stream_logger.error('Status code was {0}'.format(r.status_code))
            return None
        data = r.json()
                
        return data

    def get_clicks(self, app):

        params = {
            'since': self.startdate,
            'until': self.enddate,
            'summary': 'true',
            'event_name': 'fb_ad_network_click',
            'aggregateBy': 'COUNT',
            'breakdowns[0]': 'placement',
            'access_token': self.access_tokens[app],
        }
        url = 'https://graph.facebook.com/v2.8/{0}/app_insights/app_event/'.format(self.account_ids[app])
        r = requests.get(url, params=params)
        if r.status_code != 200:
            self.stream_logger.error('Status code was {0}'.format(r.status_code))
            return None
        data = r.json()
        return data

    def get_impressions(self, app):
        
        params = {
            'since': self.startdate,
            'until': self.enddate,
            'summary': 'true',
            'event_name': 'fb_ad_network_imp',
            'aggregateBy': 'COUNT',
            'breakdowns[0]': 'placement',
            'access_token': self.access_tokens[app],
        }
        url = 'https://graph.facebook.com/v2.8/{0}/app_insights/app_event/'.format(self.account_ids[app])
        r = requests.get(url, params=params)
        if r.status_code != 200:
            self.stream_logger.error('Status code was {0}'.format(r.status_code))
            return None
        data = r.json()
        return data

    def get_requests(self, app):
       
        params = {
            'since': self.startdate,
            'until': self.enddate,
            'summary': 'true',
            'event_name': 'fb_ad_network_request',
            'aggregateBy': 'COUNT',
            'breakdowns[0]': 'placement',
            'access_token': self.access_tokens[app],
        }
        url = 'https://graph.facebook.com/v2.8/{0}/app_insights/app_event/'.format(self.account_ids[app])
        r = requests.get(url, params=params)
        if r.status_code != 200:
            self.stream_logger.error('Status code was {0}'.format(r.status_code))
            return None
        data = r.json()
        return data

    def get_requests_resold(self, app):
       
        params = {
            'since': self.startdate,
            'until': self.enddate,
            'summary': 'true',
            'event_name': 'fb_ad_network_request',
            'aggregateBy': 'SUM',
            'breakdowns[0]': 'placement',
            'access_token': self.access_tokens[app],
        }
        url = 'https://graph.facebook.com/v2.8/{0}/app_insights/app_event/'.format(self.account_ids[app])
        r = requests.get(url, params=params)
        if r.status_code != 200:
            self.stream_logger.error('Status code was {0}'.format(r.status_code))
            return None
        data = r.json()
        return data
     
    def update_values(self, data, key):
        
        if not data:
            return
        
        for datum in data['data']:
            placement_id = datum['breakdowns']['placement']
            value = datum['value']
            if placement_id not in self.placement_dict:
                self.placement_dict[placement_id] = {
                    'revenue_usd': 0,
                    'clicks': 0,
                    'impressions': 0,
                    'ad_requests': 0,
                    'ad_requests_resold': 0,
                }

            if key == 'revenue_usd':
                value = round(float(value),10)
            elif key in ('clicks', 'impressions', 'ad_requests', 'ad_requests_resold'):
                value = int(value)
            self.placement_dict[placement_id][key] = value
                    
    def _getData(self):
        """ 
        Sends the request that retrieves the report's data.
         
        :returns: returns the report's data as dict. 
        """
        
        for app in self.app_list[:]:
                
            revenue = self.get_revenue(app)
            self.update_values(revenue, 'revenue_usd')
            self.stream_logger.debug('{1} revenue {0}'.format(pp.pformat(revenue), self.account_names[app]))
    
            clicks = self.get_clicks(app)
            self.update_values(clicks, 'clicks')
            self.stream_logger.debug('{1} clicks {0}'.format(pp.pformat(clicks), self.account_names[app]))
    
            impressions = self.get_impressions(app)
            self.update_values(impressions, 'impressions')
            self.stream_logger.debug('{1} impressions {0}'.format(pp.pformat(impressions), self.account_names[app]))
    
            ad_requests = self.get_requests(app)
            self.update_values(ad_requests, 'ad_requests')
            self.stream_logger.debug('{1} requests {0}'.format(pp.pformat(ad_requests), self.account_names[app]))
    
            ad_requests_resold = self.get_requests_resold(app)
            self.update_values(ad_requests_resold, 'ad_requests_resold')
            self.stream_logger.debug('{1} requests  resold {0}'.format(pp.pformat(ad_requests_resold), self.account_names[app]))
        
        missing_id_count = 0
        placement_name_dict = {}
        for fb_id in self.placement_dict:
            if fb_id in self.id_to_name_dict:
                key = (self.id_to_name_dict[fb_id], self.date)
                placement_name_dict[key] = self.placement_dict[fb_id]
                placement_name_dict[key].update({
                        'placement_name': key[0],
                        'date': self.date,
                        })
            else:
                self.stream_logger.error('Missing fb id mapping for {0}'.format(fb_id))
                missing_id_count += 1        
        entries = []
        for key, row_dict  in placement_name_dict.items():
            response = Response(row_dict)
            entries.append(response)
        
        return entries
    
    def date_after_days_str(self, date, days):
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date() \
            + datetime.timedelta(days)  
        return d.strftime('%Y-%m-%d')