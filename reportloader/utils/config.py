#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" 
This module provides utilities that helps in the configuration of the 
application.
"""

import configparser
import datetime
import os
import json 
import simplejson

def getConfig(file):
    """ 
    It's a helping function that encapsulate the configparser module.
    
    :param file: the file that contains the application's configuration.
    :returns: returns the config object that is instantiated.
    """
    
    config = configparser.ConfigParser()
    config.read(file)
    if len(config.sections()) == 0:
        raise Exception('Could not parse configuration file. Aborting.')     
    return config
    
class Config:
    """ 
    This class encapsulates the configuration management of the application.
    """
    
    CONFIG_FILE = "../reportloader.conf"
    config = getConfig(os.path.join(os.path.abspath(os.path.dirname(__file__)), CONFIG_FILE))
    
    @classmethod
    def getParam(cls, section, attr):
        """ 
        This method returns a config's property's value.
         
        :param section: the section of the config file.
        :param attr: the aattribute of the config file.
        :returns: returns the value that correspons to the specified 
                  section/attribute. 
        """
        env_var = '{0}_{1}'.format(section.upper(), attr.upper())
        
        if env_var in os.environ:
            return os.environ[env_var]
        
        if not attr in cls.config[section]:
            return 
        
        return cls.config[section][attr]