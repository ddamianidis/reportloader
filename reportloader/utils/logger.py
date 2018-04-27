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
  
    
class StreamLogger():
    """ 
    This class inherits the Looger class provided by the logging module.
    """
    
    @classmethod
    def getLogger(cls, name, format=None, level=logging.INFO):
        lname = "{0}{1}".format(name, time.time())
        logger = logging.getLogger(lname)
        cls.handler = logging.StreamHandler(sys.stdout)
        if not format:
            format = '[%(asctime)s - %(name)s - %(levelname)s] - %(message)s'
        formatter = logging.Formatter(format)
        cls.handler.setFormatter(formatter)
        logger.addHandler(cls.handler)
        logger.setLevel(level)
        return logger    
    
    @classmethod
    def setFormat(cls, logger, format):
        formatter = logging.Formatter(format)
        cls.handler.setFormatter(formatter)
        logger.addHandler(cls.handler)
    
    @classmethod
    def setLevel(cls, logger, level):
        logger.setLevel(level)    