#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
This module provides utilities that helps the logging of the application.
"""

import logging
import datetime
import os
import sys
import time
  
    
class PullModeKeeper():
    """ 
    This class keeps the pull mode of the platforms 
    """
    
    pull_mode_status = {
         'appnexus': 'normal',
         'adsense': 'normal',
         'adx': 'normal',
         'criteo': 'normal',
         'criteohb': 'normal',
         'teads': 'normal',
         'taboola': 'normal',
         'smart': 'normal',
         'smaato': 'normal',
         'facebook': 'normal',
         'rubicon': 'normal',
         'pubmatic': 'normal',
    }
    
    @classmethod
    def setModeAsMute(cls, platform):
        cls.pull_mode_status[platform] = 'mute' 
        return True
    
    @classmethod
    def setModeAsNormal(cls, platform):
        cls.pull_mode_status[platform] = 'normal' 
        return True    
    
    @classmethod
    def getMode(cls, platform):
        return cls.pull_mode_status[platform]
    
    @classmethod
    def isMuted(cls, platform):
        return cls.pull_mode_status[platform] is 'mute'