#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import json
import time
from unittest.mock import MagicMock
from unittest.mock import patch
from etlreporting.logger import StreamLogger


class LoggerTestCase(unittest.TestCase):
    
        
    def setUp(self):
        pass    
    
    def tearDown(self):
        pass
    
    #@unittest.skip('just skip')    
    def test_1_post_stats(self):        
        sl = StreamLogger.getLogger(__name__)
        sl.info('Hello from stream logger')
        
if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(LoggerTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)
    