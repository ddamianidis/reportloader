import json
import os
import time
import datetime
from reportloader.utils.config import Config
from unittest.mock import MagicMock
from reportloader.tests.TestCase import TestCase
from reportloader.reporterpuller import ReporterPuller

  
def main():
    
    puller = ReporterPuller("pubmatic", "2018-05-21", "2018-05-21")
    ret_data = puller.pull_data()
        
    #print(fetched_data)
if __name__ == "__main__": main()